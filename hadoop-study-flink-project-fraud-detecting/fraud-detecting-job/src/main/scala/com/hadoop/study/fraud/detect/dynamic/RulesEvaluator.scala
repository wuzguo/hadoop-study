package com.hadoop.study.fraud.detect.dynamic

import com.hadoop.study.fraud.detect.beans.{AlertEvent, Rule}
import com.hadoop.study.fraud.detect.config.Config
import com.hadoop.study.fraud.detect.config.Parameters.{CHECKPOINT_INTERVAL, LOCAL_EXECUTION, LOCAL_MODE_DISABLE_WEB_UI, MIN_PAUSE_BETWEEN_CHECKPOINTS, OUT_OF_ORDERLESS, RULES_SOURCE, SINK_PARALLELISM, SOURCE_PARALLELISM}
import com.hadoop.study.fraud.detect.functions.{AverageAggregate, DynamicAlertFunction, DynamicKeyFunction}
import com.hadoop.study.fraud.detect.sinks.{AlertsSink, CurrentRulesSink, LatencySink}
import com.hadoop.study.fraud.detect.sources.{RulesSource, SourceType, TransactionsSource}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
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

    @throws[Exception]
    def run(): Unit = {
        // Environment setup
        val env = configureStreamExecutionEnvironment()

        // Streams setup
        val rulesUpdateStream = getRulesUpdateStream(env)

        val transactions = getTransactionsStream(env)

        val rulesStream = rulesUpdateStream.broadcast(Descriptors.rulesDescriptor)

        // Processing pipeline setup
        val alerts = transactions.connect(rulesStream)
          .process(new DynamicKeyFunction)
          .uid("DynamicKeyFunction")
          .name("Dynamic Partitioning Function")
          .keyBy(_.key)
          .connect(rulesStream)
          .process(new DynamicAlertFunction)
          .uid("DynamicAlertFunction")
          .name("Dynamic Rule Evaluation Function")

        val latency:DataStream[Long] = alerts.asInstanceOf[SingleOutputStreamOperator[AlertEvent]].getSideOutput(Descriptors.latencySinkTag)
        val currentRules: DataStream[Rule] = alerts.asInstanceOf[SingleOutputStreamOperator[AlertEvent]].getSideOutput(Descriptors.currentRulesSinkTag)
        val alertsJson: DataStream[String] = AlertsSink.alertsStreamToJson(alerts)
        val currentRulesJson = CurrentRulesSink.rulesStreamToJson(currentRules)
        val sinkParallelism = config.get(SINK_PARALLELISM)
        alertsJson.addSink(AlertsSink.createAlertsSink(config)).setParallelism(sinkParallelism).name("Alerts JSON Sink")
        currentRulesJson.addSink(CurrentRulesSink.createRulesSink(config)).setParallelism(sinkParallelism).name("Rules Export Sink")

        val latencies = latency.timeWindowAll(Time.seconds(10)).aggregate(new AverageAggregate).map(value => value.toString)
        latencies.addSink(LatencySink.createLatencySink(config)).name("Latency Sink")

        env.execute("Fraud Detection Engine")
    }

    private def getTransactionsStream(env: StreamExecutionEnvironment): DataStream[Transaction] = { // Data stream setup
        val transactionSource = TransactionsSource.createTransactionsSource(config)
        val sourceParallelism = config.get(SOURCE_PARALLELISM)
        val transactionsStringsStream = env.addSource(transactionSource).name("Transactions Source").setParallelism(sourceParallelism)

        val transactionsStream = TransactionsSource.stringsStreamToTransactions(transactionsStringsStream)
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
        SourceType.withName(rulesSource.toUpperCase)
    }

    private def configureStreamExecutionEnvironment() = {
        val localMode = config.get(LOCAL_EXECUTION)
        var env: StreamExecutionEnvironment = null
        if (localMode.isEmpty || localMode == LOCAL_MODE_DISABLE_WEB_UI) { // cluster mode or disabled web UI
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