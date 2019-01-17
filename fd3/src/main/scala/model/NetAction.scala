package model

import java.sql.Date
import java.text.SimpleDateFormat

case class NetAction(sType: String, ip: String, time: Long, category_id: String, dt: Date) {
  val df = new SimpleDateFormat("yyyy.MM.dd G 'at' HH:mm:ss z")
  override def toString(): String = "type:" + sType + " ip:" + ip + " time:" + time + " category_id:" + category_id + " Date:" + df.format(dt)
}
