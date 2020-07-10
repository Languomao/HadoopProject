package structured_streaming.parse

/**
  * Classname CPUDataMessage
  * Description TODO
  * Date 2020/7/10 10:40
  * Created by LanKorment
  */

case class CPUDataMessage(time: String, namespace: String, id: String, region: String, value: String, valueType: String)
