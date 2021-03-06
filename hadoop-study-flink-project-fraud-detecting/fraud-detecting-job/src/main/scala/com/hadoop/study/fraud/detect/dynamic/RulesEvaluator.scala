package com.hadoop.study.fraud.detect.dynamic

import com.hadoop.study.fraud.detect.beans.{Rule, Transaction}
import com.hadoop.study.fraud.detect.config.Config
import com.hadoop.study.fraud.detect.config.Parameters._
import com.hadoop.study.fraud.detect.functions.{AverageFunction, DynamicAlertFunction, DynamicKeyFunction}
import com.hadoop.study.fraud.detect.sinks.{AlertsSink, LatencySink, RulesSink}
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

        // Streams setup
        val rulesStream = getRuleStream(env)
        val transactionStream = getTransactionsStream(env)

        // Broadcast
        val ruleBroadcastStream = rulesStream.broadcast(Descriptors.rulesDescriptor)

        // Processing pipeline setup
        val alertStream = transactionStream.connect(ruleBroadcastStream)
          .process(DynamicKeyFunction())
          .uid("Dynamic Key Function")
          .name("Dynamic Partitioning Function")
          .keyBy(_.key)
          .connect(ruleBroadcastStream)
          .process(DynamicAlertFunction())
          .uid("Dynamic Alert Function")
          .name("Dynamic Rule Evaluation Function")

        val currentRuleStream = alertStream.getSideOutput(Tags.currentRulesSinkTag)
        val rulesJsonStream = RulesSink.streamToJson(currentRuleStream)

        val sinkParallelism = config.get(SINK_PARALLELISM)
        rulesJsonStream.addSink(RulesSink.create(config)).setParallelism(sinkParallelism).name("Rules Sink")

        val alertsJsonStream = AlertsSink.streamToJson(alertStream)
        alertsJsonStream.addSink(AlertsSink.create(config)).setParallelism(sinkParallelism).name("Alerts Sink")

        val latenciesStream = alertStream.getSideOutput(Tags.latencySinkTag)
        latenciesStream.timeWindowAll(Time.seconds(10))
          .aggregate(AverageFunction())
          .map(_.toString)
          .addSink(LatencySink.create(config))
          .name("Latency Sink")

        env.execute("Fraud Detection Engine")
    }

    private def getTransactionsStream(env: StreamExecutionEnvironment): DataStream[Transaction] = {
        // Data stream setup
        val sourceParallelism = config.get(SOURCE_PARALLELISM)
        val sourceType = RulesSource.getSourceType(config)

        val transactionSource = TransactionsSource.create(config)
        val transactionStringsStream = env.addSource(transactionSource)
          .name(sourceType.toString)
          .setParallelism(sourceParallelism)

        val transactionsStream = TransactionsSource.streamToTransactions(transactionStringsStream)
        transactionsStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(config.get(OUT_OF_ORDERLESS).longValue()))
              .withTimestampAssigner(new SerializableTimestampAssigner[Transaction] {
                  override def extractTimestamp(element: Transaction, recordTimestamp: Long): Long = element.eventTime
              })
        )
    }

    private def getRuleStream(env: StreamExecutionEnvironment): DataStream[Rule] = {
        val sourceParallelism = config.get(SOURCE_PARALLELISM)
        val rulesSource = RulesSource.create(config)
        val sourceType = RulesSource.getSourceType(config)

        val rulesStringStream = env.addSource(rulesSource)
          .name(sourceType.toString)
          .setParallelism(sourceParallelism)

        RulesSource.streamToRules(rulesStringStream)
    }

    private def configEnvironment() = {
        val localMode = config.get(LOCAL_EXECUTION)

        var env: StreamExecutionEnvironment = null
        if (localMode.isEmpty || localMode == LOCAL_MODE_DISABLE_WEB_UI) {
            env = StreamExecutionEnvironment.getExecutionEnvironment
        } else {
            val flinkConfig = new Configuration
            flinkConfig.set(RestOptions.BIND_PORT, localMode)
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig)
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

    val currentRulesSinkTag: OutputTag[Rule] = new OutputTag[Rule]("current-rules-sink")
}