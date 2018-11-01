package com.my.frauddetection


import kafka.serializer.StringDecoder
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{kafka, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._

object FraudDetectionShort {
  val topics = Set("lab_action")
  val numThreads = 2
  val zkQuorum = "localhost:2181"
  val group = "test_lab_action_2"
  val server = "localhost:9092"
  val numItertion = 13
  val ttl = 432000// set ttl 5 days

  def main(args: Array[String]): Unit = {
    println("start" + numItertion + "!")
    //  val sparkConf = new SparkConf().setAppName("FraudDetection").set("spark.master", "local[*]")
    //    "ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("FraudDetector" + numItertion)
      .config("spark.driver.memory", "2g")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cleaner.ttl", ttl)
      .getOrCreate()
    //    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val checkpointDir = "file:///opt/checkpoint"
    val windowSize = 100
    val slideSize = 100

    val windowDuration = s"$windowSize seconds"
    val slideDuration = s"$slideSize seconds"

    val streamingContext = StreamingContext.getOrCreate(checkpointDir, () => {
      val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
      ssc.sparkContext.setLogLevel("ERROR")
      //      ssc.checkpoint(checkpointDir)
      val kafkaStream: DStream[NetAction] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, Map("metadata.broker.list" -> server, "group.id" -> group), topics )
        .map(x => FraudDetection.getJsonContent(x._2))

      kafkaStream.foreachRDD { rdd =>
        import spark.implicits._
        val wnd= window($"dt", windowDuration, slideDuration)
        val timeFrame: DataFrame = rdd.toDF("sType", "ip", "time", "category_id", "dt").repartition(5).cache()

        val result: Dataset[Row] = timeFrame.groupBy(wnd, $"ip").agg(
          collect_set("category_id").alias("categories"),
          count("*").alias("clickCount"),
          count(when($"sType" === "click", 1)).alias("cCount"),
          count(when($"sType" === "view", 1)).alias("vCount")
        ) .withColumn("div",($"cCount" / $"vCount"  ))
          .withColumn("sz",size($"categories"))
          .filter(($"div" > 5  ).or($"clickCount" > 30).or($"sz" > 10))

        result.select($"ip",$"window.start".alias("datets"), $"reason")
          .coalesce(3)
          .write
          .mode(SaveMode.Append)
          .cassandraFormat("fraud", "lab1")
          .save
      }
      ssc
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }


}
