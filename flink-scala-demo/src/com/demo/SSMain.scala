package com.demo

//import org.apache.spark.sql.SparkSession

/**
  * Created by david03.wang on 2018/1/19.
  */
object SSMain {

  def main(args: Array[String]): Unit = {

//    val host = "10.199.201.102"
//    val port = 8989
//
//    val spark = SparkSession
//      .builder
//      .appName("StructuredNetworkWordCount")
//      .master("local[2]")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    // Create DataFrame representing the stream of input lines from connection to host:port
//    val lines = spark.readStream
//      .format("socket")
//      .option("host", host)
//      .option("port", port)
//      .load()
//
//    // Split the lines into words
//    val words = lines.as[String].flatMap(_.split(" "))
//
//    // Generate running word count
//    val wordCounts = words.groupBy("value").count()
//
//    // Start running the query that prints the running counts to the console
//    val query = wordCounts.writeStream
//      .outputMode("complete")
//      .format("console")
//      .start()
//
//    query.awaitTermination()

  }


}
