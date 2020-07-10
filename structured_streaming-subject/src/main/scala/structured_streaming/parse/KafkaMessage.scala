package structured_streaming.parse

/**
  * Classname KafkaMessage
  * Description TODO
  * Date 2020/7/9 11:04
  * Created by LanKorment
  */

case class KafkaMessage(time: String, namespace: String, id: String, region: String, value: String, valueType: String)
