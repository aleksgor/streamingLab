
import java.sql.Date

import com.my.frauddetection.NetAction
import com.my.frauddetection.FraudDetection
import org.scalatest.{BeforeAndAfter, _}

class Test1 extends  FunSuite with BeforeAndAfter{


  val testString: String = "{\"type\": \"click\",\"category_id\": \"1000\",\"ip\": \"127.0.0.2\",\"unix_time\": 1500028835}"

    before {
      //
    }

    test("test generate NetAction") {

      //  "A ip" should "be in testString" in {
      val fd = FraudDetection
      val act:NetAction = fd.getJsonContent(testString)
      assert(act.ip === "127.0.0.2")
      assert(act.sType === "click")
      assert(new Date(1500028835000L).equals(act.dt))

    }


}
