
package org.apache.spark.examples.streaming

import org.apache.spark.sql.{DataFrame, SparkSession, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._


case class NetAction(sType: String, ip: String, time: Long, category_id: String) {
  override def toString(): String = "type:" + sType + " ip:" + ip + " time:" + time + " category_id:" + category_id
}

object FraudDetection {
  val topic = "lab_action"
  val topics = Set(topic)
  val numThreads = 1
  val zkQuorum = "localhost:2181"
  val group = "test_lab_action_2"
  val server = "localhost:9092"
  val numItertion = 13
  val ttl = 432000 // set ttl 5 days
  val checkpointDir = "file:///opt/checkpoint"

  def main(args: Array[String]): Unit = {
    println("start" + numItertion + "!")

    val windowSize = 100
    val slideSize = 300

    val windowDuration = s"$windowSize seconds"
    val slideDuration = s"$slideSize seconds"


    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("FraudDetection")
      .config("spark.driver.memory", "2g")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cleaner.ttl", ttl)
      .getOrCreate()

    val df: DataFrame = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .load()
    //      "type:" + sType + " ip:" + ip + " time:" + time + " category_id:" + category_id

    val schema = StructType(Seq(
      StructField("unix_time", LongType, true),
      StructField("category_id", StringType, true),
      StructField("ip", StringType, true),
      StructField("type", StringType, true)

    ))
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    df.sparkSession.sparkContext.setLogLevel("ERROR")
    val df1 = df.selectExpr("cast (value as string) as json").select(from_json($"json", schema = schema).as("data"))
      .select($"data.unix_time", $"data.category_id", $"data.ip", $"data.type", from_unixtime($"data.unix_time").cast(TimestampType).as("ts"))
      .withWatermark("ts", "1 seconds")
      .groupBy($"ip", window($"ts", slideDuration, windowDuration))
      .agg(
        collect_set("category_id").alias("categories"),
        count("*").alias("clickCount"),
        count(when($"type" === "click", 1)).alias("cCount"),
        count(when($"type" === "view", 1)).alias("vCount")
      )
      .withColumn("div",($"cCount" / $"vCount"  ))
      .withColumn("sz",size($"categories"))
      .filter(($"div" > 5  ).or($"clickCount" > 30).or($"sz" > 10))
      .select($"ip",$"window.start".alias("date"))


    df1.printSchema()
    println(df1.isStreaming)
/*
    val query = df1.writeStream
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "lab1")
      .option("table", "fraud")
      .option("spark.cleaner.ttl", ttl)
      .outputMode(OutputMode.Append())
      .start()
  */

    /*

    val query = df1.writeStream
      .outputMode("append")
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "lab1")
      .option("table", "fraud")
      .option("spark.cleaner.ttl", ttl)
      .start()

*/
    val query = df1.writeStream
      .outputMode("append")
      .queryName("table")
      .format("console")
      .start()

    query.awaitTermination()
    spark.stop()


  }
}
