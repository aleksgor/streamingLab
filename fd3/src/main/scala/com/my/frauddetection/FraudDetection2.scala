package com.my.frauddetection


import kafka.serializer.StringDecoder
import model.NetAction
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

object  FraudDetectionShort{
  def main(args: Array[String]): Unit = {
    val fd :FraudDetectionShortClass = new FraudDetectionShortClass()
    fd.start()
  }
}

class FraudDetectionShortClass extends Serializable {
  val topics = Set("lab_action")
  val numThreads = 2
  val zkQuorum = "localhost:2181"
  val group = "test_lab_action_2"
  val server = "localhost:9092"
  val numItertion = 13
  val ttl = 432000
  // set ttl 5 days
  lazy val spark: SparkSession = getSparkSession()
  val checkpointDir = "file:///opt/checkpoint"
  val windowSize = 100
  val slideSize = 100

  val windowDuration = s"$windowSize seconds"
  val slideDuration = s"$slideSize seconds"

  def getSparkSession(): SparkSession = {
    SparkSession.builder
      .master("local[*]")
      .appName("FraudDetector" + numItertion)
      .config("spark.driver.memory", "2g")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cleaner.ttl", ttl)
      .getOrCreate()
  }

  def start(): Unit = {
    println("start!")

    val streamingContext = StreamingContext.getOrCreate(checkpointDir, createStreamingContext)
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def getInputDStream(ssc: StreamingContext): InputDStream[(String, String)] = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, Map("metadata.broker.list" -> server, "group.id" -> group), topics)
  }

  def createStreamingContext(): StreamingContext = {
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    ssc.sparkContext.setLogLevel("ERROR")
    getDstream(ssc)
    ssc
  }

  def getDstream(ssc:StreamingContext): DStream[NetAction] ={
    val kafkaStream: DStream[NetAction] = getInputDStream(ssc).map(x => FraudDetectionDStream.getJsonContent(x._2))
    process(kafkaStream)
    kafkaStream
  }

  def process(stream: DStream[NetAction]):  DStream[NetAction] ={

    stream.foreachRDD { rdd =>
      import spark.implicits._
      val wnd = window($"dt", windowDuration, slideDuration)
      val timeFrame: DataFrame = rdd.toDF("sType", "ip", "time", "category_id", "dt").repartition(5).cache()
      val result: Dataset[Row] = timeFrame.groupBy(wnd, $"ip").agg(
        collect_set("category_id").alias("categories"),
        count("*").alias("clickCount"),
        count(when($"sType" === "click", 1)).alias("cCount"),
        count(when($"sType" === "view", 1)).alias("vCount")
      ).withColumn("div", ($"cCount" / $"vCount"))
        .withColumn("sz", size($"categories"))
        .filter(($"div" > 5).or($"clickCount" > 30).or($"sz" > 10))
        .select($"ip", $"window.start".alias("datets"))
      saveResult(result)
    }
    stream
  }

  def saveResult(result:Dataset[Row])={
    result.coalesce(3)
      .write
      .mode(SaveMode.Append)
      .cassandraFormat("fraud", "lab1")
      .save

  }
}
