package com.demo

import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.DeserializationSchema
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Comsumer {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    var properties = new Properties()
    properties.put("zookeeper.connect", "10.199.202.161:2181")
    properties.put("bootstrap.servers", "10.199.202.161:9092")
    properties.put("group.id", "dxwang-only-test")
    properties.put("offsets.storage", "kafka")
    properties.put("enable.auto.commit", "true")
    properties.put("auto.commit.interval.ms", "1000")

    val msgStream = env.addSource(new FlinkKafkaConsumer08("test", new POjoDeser, properties))
    val streamF = msgStream.map(pojo => (pojo.name, pojo.age, System.currentTimeMillis()))
    val stramWithWatermark = streamF.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long, Long)] {
      var watermark: Long = -1

      override def getCurrentWatermark = new Watermark(watermark)

      override def extractTimestamp(element: (String, Long, Long), previousElementTimestamp: Long) = {
        this.watermark = element._3
        this.watermark
      }
    })

    val table = stramWithWatermark.toTable(tableEnv, 'name, 'age, 'rowtime.rowtime)
    tableEnv.registerTable("t1", table)
    val sqlResult = tableEnv.sqlQuery("select name, count(*) as age from t1 group by name, TUMBLE(rowtime, interval '10' second)")
    sqlResult.toAppendStream(TypeExtractor.createTypeInfo(classOf[Pojo])).print()

    env.execute()

  }

}

class POjoDeser extends DeserializationSchema[Pojo] {

  override def isEndOfStream(nextElement: Pojo): Boolean = false

  override def deserialize(message: Array[Byte]): Pojo = {
    var p = new Pojo
    p.age = 1
    p.name = new String(message)
    p
    //    Pojo(new String(message),1)
  }

  override def getProducedType: TypeInformation[Pojo] = {
    TypeExtractor.getForClass(classOf[Pojo])
  }
}

//case class Pojo(@BeanProperty name: String,@BeanProperty age: Long)

class Pojo {
  var name: String = _
  var age: Long = _

  def getName: String = this.name

  def getAge: Long = this.age

  def setName(name: String) = {
    this.name = name
  }

  def setAge(age: Long) = {
    this.age = age
  }

  override def toString(): String = {
    s"""(name: $name, age: $age)"""
  }
}

