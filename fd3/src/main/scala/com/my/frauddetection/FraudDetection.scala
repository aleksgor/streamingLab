package com.my.frauddetection

import java.sql.Date
import java.text.SimpleDateFormat

import kafka.serializer.StringDecoder
import model.NetAction
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.apache.spark.sql.cassandra._

object FraudDetectionDStream {
  val topics = Set("lab_action")
  val numThreads = 2
  val zkQuorum = "localhost:2181"
  val group = "test_lab_action_2"
  val server = "localhost:9092"
  val ttl = 432000// set ttl 5 days
  val windowSize = 300
  val slideSize = 300
  val windowDuration = s"$windowSize seconds"
  val slideDuration = s"$slideSize seconds"

  def main(args: Array[String]): Unit = {
    //  val sparkConf = new SparkConf().setAppName("FraudDetection").set("spark.master", "local[*]")
    //    "ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("FraudDetector")
      .config("spark.driver.memory", "2g")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cleaner.ttl", ttl)
      .getOrCreate()
    //    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val checkpointDir = "file:///opt/checkpoint"


    val streamingContext = StreamingContext.getOrCreate(checkpointDir, () => {
      val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
      ssc.sparkContext.setLogLevel("ERROR")

      val kafkaStream: DStream[NetAction] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, Map("metadata.broker.list" -> server, "group.id" -> group), topics )
        .map(x => getJsonContent(x._2))
      process(kafkaStream, spark, saveResult)
      ssc
    })
//    println("-1"+streamingContext.)
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def process(kafkaStream: DStream[NetAction], spark: SparkSession, saveResult : DataFrame=>Unit ):Unit={

    println("rdd2:" + kafkaStream)
    kafkaStream.foreachRDD { rdd =>
      import spark.implicits._
      println("rdd:")
      rdd.foreach(x=> println())
      val wnd= window($"dt", windowDuration, slideDuration)
      val timeFrame: DataFrame = rdd.toDF("sType", "ip", "time", "category_id", "dt").repartition(5).cache()
      println("rdd1:")

      val windowedCounts: Dataset[Row] = timeFrame.groupBy(wnd, $"ip")
        .agg(count("*").as("count"))
        .filter("count > 35")
        .select("window","ip").withColumn("reason",lit("1"))
      println("rdd2:")

      val categoryCount = timeFrame.groupBy(wnd, $"ip", $"category_id").agg(count("category_id").as("rating"))
        .groupBy("window", "ip").agg(max("rating").as("maxr")).filter("maxr > 6")
        .select("window","ip").withColumn("reason",lit("2"))
      println("rdd3:")

      val viewDs: Dataset[Row]  = timeFrame.filter( $"sType" === "view")
        .groupBy(wnd, $"ip")
        .agg(count("*").as("viewCnt"))
      println("rdd4:")

      val clickDs:  Dataset[Row] = timeFrame.filter($"sType" === "click")
        .groupBy(wnd, $"ip")
        .agg(count("*").as("clickCnt"))
      println("rdd5:")

      val clickView: DataFrame= clickDs.join(viewDs, clickDs.col("window").equalTo(viewDs.col("window")).and(clickDs.col("ip").equalTo(viewDs.col("ip"))),"leftouter" )
        .select( clickDs.col("window"), clickDs.col("ip"), $"viewCnt" ,$"clickCnt" ).na.fill(1,Seq("viewCnt"))
        .filter(col("clickCnt")/col("viewCnt") < 30 )
        .select("window","ip").withColumn("reason",lit("3"))

      val result :  DataFrame= windowedCounts.union(categoryCount).union(clickView).select(col("ip"),col("reason"), col("window.end").alias("date"))
      saveResult(result)
    }
  }

  def saveResult(result :  DataFrame): Unit={
    result
      .coalesce(3)
      .write
      .mode(SaveMode.Append)
      .cassandraFormat("fraud", "lab1")
      .save

  }
  def getJsonContent(jsonString: String): NetAction = {
    var cleanJson = jsonString
    if (jsonString.startsWith("[")) {
      cleanJson = jsonString.substring(1)
    }else if(jsonString.endsWith("]")){
      cleanJson = jsonString.slice(0, jsonString.length - 1)
    }

    implicit val formats = DefaultFormats
    val parsedJson = parse(jsonString)
    val sType = (parsedJson \ "type").extract[String]
    val ip = (parsedJson \ "ip").extract[String]
    val time: Long = (parsedJson \ "unix_time").extract[Long]
    val categoryId = (parsedJson \ "category_id").extract[String]
    val dt = new Date(time * 1000)

    NetAction(sType, ip, time, categoryId, dt)
  }

}
