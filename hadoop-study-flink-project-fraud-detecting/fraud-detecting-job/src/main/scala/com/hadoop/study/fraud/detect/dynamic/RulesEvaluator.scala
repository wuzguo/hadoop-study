package com.hadoop.study.fraud.detect.dynamic

import com.hadoop.study.fraud.detect.beans.{Rule, Transaction}
import com.hadoop.study.fraud.detect.config.Config
import com.hadoop.study.fraud.detect.config.Parameters._
import com.hadoop.study.fraud.detect.enums.SourceType
import com.hadoop.study.fraud.detect.functions.{AverageAggregate, DynamicAlertFunction, DynamicKeyFunction}
import com.hadoop.study.fraud.detect.sinks.{AlertsAbstractSink$, LatencyAbstractSink$, RulesAbstractSink$}
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
        val env = configEnvironment()
        log.info("rules run env: {}", env)

        // Streams setup
        val rulesStream = getRuleStream(env)
        val transactionStream = getTransactionsStream(env)

        val ruleBroadcastStream = rulesStream.broadcast(Descriptors.rulesDescriptor)

        // Processing pipeline setup
        val alertStream = transactionStream.connect(ruleBroadcastStream)
          .process(new DynamicKeyFunction)
          .uid("Dynamic Key Function")
          .name("Dynamic Partitioning Function")
          .keyBy(_.key)
          .connect(ruleBroadcastStream)
          .process(new DynamicAlertFunction)
          .uid("Dynamic Alert Function")
          .name("Dynamic Rule Evaluation Function")

        val currentRuleStream = alertStream.getSideOutput(Tags.rulesSinkTag)
        val rulesJsonStream = RulesAbstractSink$.streamToJson(currentRuleStream)

        val sinkParallelism = config.get(SINK_PARALLELISM)
        rulesJsonStream.addSink(RulesAbstractSink$.create(config)).setParallelism(sinkParallelism).name("Rules Sink")

        val alertsJsonStream = AlertsAbstractSink$.streamToJson(alertStream)
        alertsJsonStream.addSink(AlertsAbstractSink$.create(config)).setParallelism(sinkParallelism).name("Alerts Sink")

        val latenciesStream = alertStream.getSideOutput(Tags.latencySinkTag)
        latenciesStream.timeWindowAll(Time.seconds(10))
          .aggregate(AverageAggregate())
          .map(_.toString)
          .addSink(LatencyAbstractSink$.create(config))
          .name("Latency Sink")

        env.execute("Fraud Detection Engine")
    }

    private def getTransactionsStream(env: StreamExecutionEnvironment): DataStream[Transaction] = {
        // Data stream setup
        val transactionSource = TransactionsSource.create(config)
        val sourceParallelism = config.get(SOURCE_PARALLELISM)

        val transactionsStringsStream = env.addSource(transactionSource)
          .name("Transactions Source")
          .setParallelism(sourceParallelism)

        val transactionsStream = TransactionsSource.streamToTransactions(transactionsStringsStream)
        transactionsStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(config.get(OUT_OF_ORDERLESS).longValue()))
              .withTimestampAssigner(new SerializableTimestampAssigner[Transaction] {
                  override def extractTimestamp(element: Transaction, recordTimestamp: Long): Long = element.eventTime
              })
        )
    }

    private def getRuleStream(env: StreamExecutionEnvironment): DataStream[Rule] = {
        val sourceType = RulesSource.getSourceType(config)
        val rulesSource = RulesSource.create(config)
        val rulesStrings = env.addSource(rulesSource).name(sourceType.toString).setParallelism(1)
        RulesSource.streamToRules(rulesStrings)
    }

    private def configEnvironment() = {
        val localMode = config.get(LOCAL_EXECUTION)

        val env: StreamExecutionEnvironment = if (localMode.isEmpty || localMode == LOCAL_MODE_DISABLE_WEB_UI) {
            StreamExecutionEnvironment.getExecutionEnvironment
        } else {
            val flinkConfig = new Configuration
            flinkConfig.set(RestOptions.BIND_PORT, localMode)
            StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig)
        }

        // 重启策略配置 // 固定延迟重启（隔一段时间尝试重启一次）
        // (尝试重启次数， 尝试重启的时间间隔)
        if (localMode.nonEmpty) {
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 10 * 1000))
        }
        // 执行超时时间
        env.getCheckpointConfig.setCheckpointTimeout(config.get(CHECKPOINT_TIMEOUT).longValue())
        // CP周期
        env.getCheckpointConfig.setCheckpointInterval(config.get(CHECKPOINT_INTERVAL).longValue())
        // 两次执行的最小间隔时间
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(config.get(MIN_PAUSE_BETWEEN_CHECKPOINTS).longValue())

        env
    }
}

object Descriptors {
    val rulesDescriptor = new MapStateDescriptor[Int, Rule]("rules", classOf[Int], classOf[Rule])
}

object Tags {
    val latencySinkTag: OutputTag[Long] = new OutputTag[Long]("latency-sink")

    val rulesSinkTag: OutputTag[Rule] = new OutputTag[Rule]("current-rules-sink")
}