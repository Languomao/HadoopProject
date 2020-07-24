package structured_streaming.parse

import java.util.Properties
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import scala.io.Source

/**
  * Classname KafkaProducer
  * Description TODO
  * Date 2020/7/21 15:05
  * Created by LanKorment
  */
object KafkaProducer {
  def main(args: Array[String]): Unit = {
    //kafka生产消息
    messageproducer("D:\\WorkSpace\\Telemetry\\test.txt","test")
  }

  //传入的参数path为经过预处理的数据的路径，即kafka会将该路径下的数据生成消息,topic为kafka主题
  def messageproducer(path : String , topic : String){
    val kafkaProp = new Properties()
    kafkaProp.put("bootstrap.servers", "localhost:9092")
    kafkaProp.put("acks", "1")
    kafkaProp.put("retries", "3")
    //kafkaProp.put("batch.size", 16384)//16k
    kafkaProp.put("key.serializer", classOf[StringSerializer].getName)
    kafkaProp.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](kafkaProp)
    val lines = Source.fromFile(path).getLines()
    while (lines.hasNext) {
      val line = lines.next()
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (metadata != null) {
            println("消息发送成功")
          }
          if (exception != null) {
            println("消息发送失败")
          }
        }
      })
    }
    producer.close()
  }
}
