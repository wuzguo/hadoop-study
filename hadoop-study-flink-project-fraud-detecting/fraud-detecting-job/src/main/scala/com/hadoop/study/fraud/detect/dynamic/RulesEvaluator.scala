package com.hadoop.study.fraud.detect.dynamic

import com.hadoop.study.fraud.detect.beans.{Rule, Transaction}
import com.hadoop.study.fraud.detect.config.Config
import com.hadoop.study.fraud.detect.config.Parameters._
import com.hadoop.study.fraud.detect.enums.SourceType
import com.hadoop.study.fraud.detect.functions.{AverageAggregate, DynamicAlertFunction, DynamicKeyFunction}
import com.hadoop.study.fraud.detect.sinks.{AlertsSink, CurrentRulesSink, LatencySink}
import com.hadoop.study.fraud.detect.sources.{RulesSource, TransactionsSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.LoggerFactory

import java.time.Duration

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 9:27
 */

case class RulesEvaluator(config: Config) {

    private val log = LoggerFactory.getLogger(classOf[RulesEvaluator])

    def run(): Unit = {
        // Environment setup
        val env = configureStreamExecutionEnvironment()
        log.info("rules run env: {}", env)
        // Streams setup
        val rulesUpdateStream = getRulesUpdateStream(env)

        val transactions = getTransactionsStream(env)
        val rulesStream = rulesUpdateStream.broadcast(Descriptors.rulesDescriptor)

        // Processing pipeline setup
        val alertSteam = transactions.connect(rulesStream)
          .process(new DynamicKeyFunction)
          .uid("Dynamic Key Function")
          .name("Dynamic Partitioning Function")
          .keyBy(_.key)
          .connect(rulesStream)
          .process(new DynamicAlertFunction)
          .uid("Dynamic Alert Function")
          .name("Dynamic Rule Evaluation Function")

        val currentRules = alertSteam.getSideOutput(Tags.currentRulesSinkTag)
        val currentRulesJson = CurrentRulesSink.rulesStreamToJson(currentRules)
        val sinkParallelism = config.get(SINK_PARALLELISM)
        currentRulesJson.addSink(CurrentRulesSink.createRulesSink(config)).setParallelism(sinkParallelism).name("Rules Export Sink")

        val alertsJson = AlertsSink.alertsStreamToJson(alertSteam)
        alertsJson.addSink(AlertsSink.createAlertsSink(config)).setParallelism(sinkParallelism).name("Alerts JSON Sink")

        val latency = alertSteam.getSideOutput(Tags.latencySinkTag)
        latency.timeWindowAll(Time.seconds(10))
          .aggregate(AverageAggregate())
          .map(_.toString)
          .addSink(LatencySink.createLatencySink(config))
          .name("Latency Sink")


        env.execute("Fraud Detection Engine")
    }

    private def getTransactionsStream(env: StreamExecutionEnvironment): DataStream[Transaction] = {
        // Data stream setup
        val transactionSource = TransactionsSource.createTransactionsSource(config)
        val sourceParallelism = config.get(SOURCE_PARALLELISM)
        val transactionsStringsStream = env.addSource(transactionSource).name("Transactions Source").setParallelism(sourceParallelism)

        val transactionsStream = TransactionsSource.streamToTransactions(transactionsStringsStream)
        transactionsStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(config.get(OUT_OF_ORDERLESS).longValue()))
              .withTimestampAssigner(new SerializableTimestampAssigner[Transaction] {
                  override def extractTimestamp(element: Transaction, recordTimestamp: Long): Long = element.eventTime
              })
        )
    }

    private def getRulesUpdateStream(env: StreamExecutionEnvironment) = {
        val rulesSourceEnumType = getRulesSourceType
        val rulesSource = RulesSource.create(config)
        val rulesStrings = env.addSource(rulesSource).name(rulesSourceEnumType.toString).setParallelism(1)
        RulesSource.streamToRules(rulesStrings)
    }

    private def getRulesSourceType = {
        val rulesSource = config.get(RULES_SOURCE)
        SourceType.withName(rulesSource.toUpperCase)
    }

    private def configureStreamExecutionEnvironment() = {
        val localMode = config.get(LOCAL_EXECUTION)

        val env: StreamExecutionEnvironment =
            if (localMode.isEmpty || localMode == LOCAL_MODE_DISABLE_WEB_UI) {
                StreamExecutionEnvironment.getExecutionEnvironment
            } else {
                val flinkConfig = new Configuration
                flinkConfig.set(RestOptions.BIND_PORT, localMode)
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig)
            }

        // slower restarts inside the IDE and other local runs 10s
        if (localMode.nonEmpty) {
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 10 * 1000))
        }
        env.getCheckpointConfig.setCheckpointInterval(config.get(CHECKPOINT_INTERVAL).longValue())
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(config.get(MIN_PAUSE_BETWEEN_CHECKPOINTS).longValue())

        env
    }
}

object Descriptors {

    val rulesDescriptor = new MapStateDescriptor[Int, Rule]("rules", classOf[Int], classOf[Rule])
}

object Tags {

    val latencySinkTag: OutputTag[Long] = new OutputTag[Long]("latency-sink")

    val currentRulesSinkTag: OutputTag[Rule] = new OutputTag[Rule]("current-rules-sink")
}