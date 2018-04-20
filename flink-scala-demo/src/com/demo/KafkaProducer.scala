package com.demo


import java.util.{Properties, Random}

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08

/**
  * Created by david03.wang on 2017/12/21.
  */
object KafkaProducer {

  def main(args: Array[String]): Unit = {

    var names: Array[String] = new Array[String](5)
    names(0) = "david"
    names(1) = "tina"
    names(2) = "bill"
    names(3) = "jack"
    names(4) = "alx"

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))

    val messageStream = env.addSource(new SourceFunction[String] {
      var running: Boolean = true

      override def cancel(): Unit = {
        this.running = false
      }

      override def run(ctx: SourceContext[String]): Unit = {
        while (this.running) {
          val msg = names(new Random().nextInt(names.length - 1))
          println(msg)
          ctx.collect(msg)
          Thread.sleep(100)
        }
      }
    })

    val properties = new Properties
    properties.put("zookeeper.connect", "10.199.202.161:2181")
    properties.put("bootstrap.servers", "10.199.202.161:9092")

    // write data into Kafka
    messageStream.addSink(new FlinkKafkaProducer08("test", new SimpleStringSchema(), properties))

    env.execute()

  }

}
