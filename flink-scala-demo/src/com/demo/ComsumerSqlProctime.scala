package com.demo

import java.util.Properties

import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

/**
  * Created by david03.wang on 2017/12/22.
  */
object ComsumerSqlProctime {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    var properties = new Properties()
    properties.put("zookeeper.connect", "10.199.202.161:2181")
    properties.put("bootstrap.servers", "10.199.202.161:9092")
    properties.put("group.id", "dxwang-only-test")
    properties.put("offsets.storage", "kafka")
    properties.put("enable.auto.commit", "true")
    properties.put("auto.commit.interval.ms", "1000")

    val msgStream = env.addSource(new FlinkKafkaConsumer08("test", new POjoDeser, properties))
    //    val streamF = msgStream.map(pojo => (pojo.name, pojo.age, System.currentTimeMillis()))

    tableEnv.registerDataStream("t1", msgStream, 'name, 'age, 'proctime.proctime)

    val sqlResult = tableEnv.sqlQuery("select name, count(*) as age from t1 group by name, TUMBLE(proctime, interval '10' second)")
    sqlResult.toAppendStream(TypeExtractor.createTypeInfo(classOf[Pojo])).print()

    env.execute()
  }


}
