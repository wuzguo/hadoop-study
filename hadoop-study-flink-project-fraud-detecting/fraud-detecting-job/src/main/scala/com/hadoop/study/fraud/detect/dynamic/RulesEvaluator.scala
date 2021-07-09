package com.hadoop.study.fraud.detect.dynamic

import com.hadoop.study.fraud.detect.beans.{AlertEvent, Rule}
import com.hadoop.study.fraud.detect.config.Config
import com.hadoop.study.fraud.detect.config.Parameters._
import com.hadoop.study.fraud.detect.functions.{AverageAggregate, DynamicAlertFunction, DynamicKeyFunction}
import com.hadoop.study.fraud.detect.sinks.{AlertsSink, CurrentRulesSink, LatencySink}
import com.hadoop.study.fraud.detect.sources.{RulesSource, RuleType, TransactionsSource}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.LoggerFactory

import java.io.IOException

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

        val currentRules = alertSteam.getSideOutput(Descriptors.currentRulesSinkTag)
        val currentRulesJson = CurrentRulesSink.rulesStreamToJson(currentRules)
        val sinkParallelism = config.get(SINK_PARALLELISM)
        currentRulesJson.addSink(CurrentRulesSink.createRulesSink(config)).setParallelism(sinkParallelism).name("Rules Export Sink")

        val alertsJson = AlertsSink.alertsStreamToJson(alertSteam)
        alertsJson.addSink(AlertsSink.createAlertsSink(config)).setParallelism(sinkParallelism).name("Alerts JSON Sink")

        val latency = alertSteam.getSideOutput(Descriptors.latencySinkTag)
        latency.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
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

        val transactionsStream = TransactionsSource.streamToTransactions(transactionsStringsStream, log)
        transactionsStream.assignTimestampsAndWatermarks(SimpleBoundedOutOfOrdernessTimestampExtractor[Transaction](config.get(OUT_OF_ORDERLESS)))
    }

    @throws[IOException]
    private def getRulesUpdateStream(env: StreamExecutionEnvironment) = {
        val rulesSourceEnumType = getRulesSourceType
        val rulesSource = RulesSource.create(config)
        val rulesStrings = env.addSource(rulesSource).name(rulesSourceEnumType.toString).setParallelism(1)
        RulesSource.streamToRules(rulesStrings)
    }

    private def getRulesSourceType = {
        val rulesSource = config.get(RULES_SOURCE)
        RuleType.withName(rulesSource.toUpperCase)
    }

    private def configureStreamExecutionEnvironment() = {
        val localMode = config.get(LOCAL_EXECUTION)
        var env: StreamExecutionEnvironment = null

        // cluster mode or disabled web UI
        if (localMode.isEmpty || localMode == LOCAL_MODE_DISABLE_WEB_UI) {
            env = StreamExecutionEnvironment.getExecutionEnvironment
        } else {
            val flinkConfig = new Configuration
            flinkConfig.set(RestOptions.BIND_PORT, localMode)
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig)
        }
        if (localMode.nonEmpty) { // slower restarts inside the IDE and other local runs
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)))
        }
        env.getCheckpointConfig.setCheckpointInterval(config.get(CHECKPOINT_INTERVAL))
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(config.get(MIN_PAUSE_BETWEEN_CHECKPOINTS))
        env
    }
}

case class SimpleBoundedOutOfOrdernessTimestampExtractor[T <: Transaction](val outOfOrderdnessMillis: Int) extends
  BoundedOutOfOrdernessTimestampExtractor[T](Time.of(outOfOrderdnessMillis, TimeUnit.MILLISECONDS)) {
    override def extractTimestamp(element: T): Long = element.eventTime
}

object Descriptors {
    val rulesDescriptor = new MapStateDescriptor[Int, Rule]("rules", classOf[Int], classOf[Rule])

    val latencySinkTag: OutputTag[Long] = new OutputTag[Long]("latency-sink")

    val currentRulesSinkTag: OutputTag[Rule] = new OutputTag[Rule]("current-rules-sink")
}