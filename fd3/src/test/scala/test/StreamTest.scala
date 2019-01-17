package test

import com.my.stream.StreamFraudDetection
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.language.postfixOps
object TestData{

  val data: List[String] = List(
    "{\"unix_time\": 1541583667, \"category_id\": 1001, \"ip\": \"172.10.0.116\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1002, \"ip\": \"172.10.0.116\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1003, \"ip\": \"172.10.0.116\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1004, \"ip\": \"172.10.0.116\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.116\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1006, \"ip\": \"172.10.0.116\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1007, \"ip\": \"172.10.0.116\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1008, \"ip\": \"172.10.0.116\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1009, \"ip\": \"172.10.0.116\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1010, \"ip\": \"172.10.0.116\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1011, \"ip\": \"172.10.0.116\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1012, \"ip\": \"172.10.0.116\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1001, \"ip\": \"172.10.0.117\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1002, \"ip\": \"172.10.0.117\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1003, \"ip\": \"172.10.0.117\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1004, \"ip\": \"172.10.0.117\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1006, \"ip\": \"172.10.0.117\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1007, \"ip\": \"172.10.0.117\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1008, \"ip\": \"172.10.0.117\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1009, \"ip\": \"172.10.0.117\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1010, \"ip\": \"172.10.0.117\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1011, \"ip\": \"172.10.0.117\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.116\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.116\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.116\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.116\", \"type\": \"view\"},",

    "{\"unix_time\": 1541583667, \"category_id\": 1003, \"ip\": \"172.10.0.119\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1003, \"ip\": \"172.10.0.119\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1003, \"ip\": \"172.10.0.119\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1003, \"ip\": \"172.10.0.119\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.119\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.119\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1006, \"ip\": \"172.10.0.119\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1009, \"ip\": \"172.10.0.119\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1009, \"ip\": \"172.10.0.119\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1009, \"ip\": \"172.10.0.119\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1011, \"ip\": \"172.10.0.119\", \"type\": \"view\"},",

    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.116\", \"type\": \"view\"}")

  val data2: List[String] = List(
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.116\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.116\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.116\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.116\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.116\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.116\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.116\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"}")

  val data3: List[String] = List(
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.116\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.116\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.116\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.116\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.116\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.116\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.116\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.117\", \"type\": \"click\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"},",
    "{\"unix_time\": 1541583667, \"category_id\": 1005, \"ip\": \"172.10.0.118\", \"type\": \"view\"}")
}

class StreamTest extends FlatSpec with Matchers with Eventually with BeforeAndAfter {

  private val master = "local[1]"
  private val appName = "spark-streaming-test"

  var sparkSession: SparkSession =_



  before {
    val conf = new SparkConf()
      .set("spark.streaming.clock", "org.apache.spark.streaming.util.TestManualClock")
      .setMaster(master).setAppName(appName)
    sparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
  }

  after {
    if (sparkSession != null) {
      sparkSession.stop()
    }
  }

  def getRdd( data: List[String]):  RDD[Row]={
    sparkSession.sparkContext.parallelize(data.map(s => Row(s)))
  }

  "Too many Categories from one ip" should "be two" in {
    val rdd: RDD[Row] = getRdd(TestData.data)
    val schema = new StructType().add(StructField("value", StringType))
    val df:DataFrame = StreamFraudDetection.process(sparkSession.createDataFrame(rdd,schema), sparkSession)
    assert(df.count() === 2)
  }

  "Clicks a lot more than views" should "be two" in {
    val rdd: RDD[Row] = getRdd(TestData.data2)
    val schema = new StructType().add(StructField("value", StringType))
    val df:DataFrame = StreamFraudDetection.process(sparkSession.createDataFrame(rdd,schema), sparkSession)
    assert(df.count() === 2)
  }

  "Too many Click from one ip" should "be one" in {
    val rdd: RDD[Row] = getRdd(TestData.data3)
    val schema = new StructType().add(StructField("value", StringType))
    val df:DataFrame = StreamFraudDetection.process(sparkSession.createDataFrame(rdd,schema), sparkSession)
    assert(df.count() === 1)
  }

}
