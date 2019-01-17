package test

import com.holdenkarau.spark.testing.StreamingSuiteBase
import com.my.frauddetection.{FraudDetectionDStream, FraudDetectionShortClass}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.{BeforeAndAfter, FunSuite}

object resultData {
  var counter:Long = 0
  def clean():Unit= counter=0
  def increase():Unit= counter=counter+1
}

class FraudDetectionShortClassTest() extends FraudDetectionShortClass{

  override def saveResult(result: Dataset[Row]):Unit= {
      result.foreach(_ => resultData.increase() )
  }
}

class DstreamTest  extends FunSuite with StreamingSuiteBase with BeforeAndAfter {

  val streamingOperations: FraudDetectionShortClassTest = new FraudDetectionShortClassTest()

  test("Dstream Too many Categories from one ip") {
    resultData.clean()
    val input1 = List(TestData.data.map(x => FraudDetectionDStream.getJsonContent(x)))
    testOperation(input1, streamingOperations.process _, input1, ordered = true )
    assert(resultData.counter == 2)
  }

  test("Dstream Clicks a lot more than views") {
    resultData.clean()
    val input1 = List(TestData.data2.map(x => FraudDetectionDStream.getJsonContent(x)))
    testOperation(input1, streamingOperations.process _, input1, ordered = true )
    assert(resultData.counter == 1)
  }
  test("Too many Click from one ip") {
    resultData.clean()
    val input1 = List(TestData.data3.map(x => FraudDetectionDStream.getJsonContent(x)))
    testOperation(input1, streamingOperations.process _, input1, ordered = true )
    assert(resultData.counter == 1)
  }


}

