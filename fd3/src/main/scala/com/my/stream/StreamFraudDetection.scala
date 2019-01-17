package com.my.stream


import java.io.FileInputStream
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{DataFrame, ForeachWriter, SparkSession, _}


object StreamFraudDetection {
  var topic = ""
  var server = ""
  var ttl = 0 // set ttl 5 days
  var checkpointDir = ""
  var tableName = ""
  val windowSize = 300
  val slideSize = 300

  val windowDuration = s"$windowSize seconds"
  val slideDuration = s"$slideSize seconds"

  val schema = StructType(Seq(
    StructField("unix_time", LongType),
    StructField("category_id", StringType),
    StructField("ip", StringType),
    StructField("type", StringType)

  ))

  def loadProp(filename: String): Unit = {
    val props: Properties = new Properties()
    props.load(new FileInputStream(filename))
    topic = props.getProperty("topic", "lab_action1")
    server = props.getProperty("sparkServer", "localhost:9093")
    checkpointDir = props.getProperty("checkpointDir", "file:///opt/checkpoint")
    ttl = props.getProperty("ttl", "600").toInt
    tableName = props.getProperty("tableName", "lab1.fraud")
  }


  def main(args: Array[String]): Unit = {
    println("start !")
    args.length match {
      case 1 =>
        println("Load file configuration: " + args(0))
        loadProp(args(0))
      case 4 =>
        println("load parameters!")
        topic = args(0)
        server = args(1)
        checkpointDir = args(2)
        ttl = args(3).toInt

      case _ =>
        println("call format: 1 parameter with configuration file name or 4 parameters with ")
        println("1: topic name, 2: sparkServer Url, 3:checkpointDir, 4: ttl. 5: cassandra table name")
        System.exit(1)

    }
    args.foreach(x => println(x))


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
      .option("kafka.bootstrap.servers", server)
      .option("subscribe", topic)
      .load()
    //      "type:" + sType + " ip:" + ip + " time:" + time + " category_id:" + category_id

    val spark:SparkSession = SparkSession.builder().getOrCreate()
    df.sparkSession.sparkContext.setLogLevel("ERROR")

    val df1 :DataFrame = process(df, spark)

    val writer = getWriter(spark)

    val query = df1.writeStream
      .queryName("server-logs processor")
      .foreach(writer)
      .start

    query.awaitTermination()

    spark.stop()

  }

  def process(df:DataFrame, spark:SparkSession):DataFrame={
    import spark.implicits._

    val viewClickDetector = spark.udf.register("viewClickDetector",(numerator: Double, denominator:Double) => {
      val correctedDenominator = if ( denominator == 0 )  1.0 else denominator
      numerator/correctedDenominator
    }  )

    df.selectExpr("cast (value as string) as json").select(from_json($"json", schema = schema).as("data"))
      .select($"data.unix_time", $"data.category_id", $"data.ip", $"data.type", from_unixtime($"data.unix_time").cast(TimestampType).as("ts"))
      .withWatermark("ts", "1 seconds")
      .groupBy($"ip", window($"ts", slideDuration, windowDuration))
      .agg(
        collect_set("category_id").alias("categories"),
        count("*").alias("clickCount"),
        count(when($"type" === "click", 1)).alias("cCount"),
        count(when($"type" === "view", 1)).alias("vCount")
      )
      //      .withColumn("vCount1", when(col("vCount").equalTo(0), 1).otherwise(col("vCount")))
      .withColumn("div", viewClickDetector($"cCount" , $"vCount"))
      .withColumn("sz", size($"categories"))

      .drop("vCount", "cCount", "categories")
      .filter(($"div" > 5).or($"clickCount" > 30).or($"sz" > 10))
      .select($"ip", $"window.start".alias("date"))

    /*

    val query = df1.writeStream
      .outputMode("append")
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "lab1")
      .option("table", "fraud")
      .option("spark.cleaner.ttl", ttl)
      .start()

*/
    /*
    val query = df1.writeStream
      .outputMode("append")
      .queryName("table")
      .format("console")
      .start()
*/
  }
  def getWriter( spark:SparkSession):ForeachWriter[Row]={

    val connector = CassandraConnector.apply(spark.sparkContext.getConf)

    val writer: ForeachWriter[Row] = new ForeachWriter[Row] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Row): Unit = {
        println(value)
        connector.withSessionDo { session =>
          session.execute(toCql(value.getString(value.fieldIndex("ip")), value.getTimestamp(value.fieldIndex("date"))))
        }
      }

      override def close(errorOrNull: Throwable): Unit = {}


      def toCql(ip: String, date: Timestamp): String = {
        val tsSting = toTimeStamp(date)
        s"""insert into $tableName (ip, datets) values('$ip', '$tsSting') USING TTL $ttl"""
      }

      def toTimeStamp(input: Timestamp): String = {
        val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
        df.format(input)
      }

    }
    writer
  }
}
