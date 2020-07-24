package structured_streaming.parse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import structured_streaming.parse.KafkaProducer.messageproducer
import structured_streaming.parse.Pretreatment.preprocessingData

/**
  * Classname StructuredStreaming4Statistic
  * Date 2020/7/17 17:25
  * Created by LanKorment
  * 解析Statistic的数据，生成DataFrame列
  * 需要修改的参数是kafka的参数
  */
object StructuredStreaming4Statistic {
  def main(args: Array[String]) {

    preprocessingData("D:\\WorkSpace\\Telemetry\\dump-statistic.txt","D:\\WorkSpace\\Telemetry\\test4.txt")

    //kafka生产消息
    messageproducer("D:\\WorkSpace\\Telemetry\\test4.txt","test4")

    //新建SparkSession，设定参数
    val spark = SparkSession
      .builder
      .appName("CameliaTest")
      .master("local[*]")
      .getOrCreate()

    //根据数据格式定义schema数据结构，此处坑多，注意数据类型以及数据的层次结构
    // 如果从kafka获取的数据是null的或者空的，先检查数据类型有没有错误,只要有一个值的类型不匹配，获得的值都会为null，然后检查数据是否可以正常解析
    val schema = new StructType()
      .add("Source",StringType)
      .add("Telemetry",new StructType()
        .add("node_id_str",StringType)
        .add("subscription_id_str",StringType)
        .add("encoding_path",StringType)
        .add("collection_id",IntegerType)
        .add("collection_start_time",TimestampType)
        .add("msg_timestamp",TimestampType)
        .add("collection_end_time",TimestampType))
      .add("Rows",ArrayType(new StructType()
        .add("Timestamp",TimestampType)
        .add("Keys",new StructType()
          .add("interface-name",StringType))
        .add("Content",new StructType()
          .add("aborted-packet-drops",LongType)
          .add("buffer-underrun-packet-drops",LongType)
          .add("dropped-ether-stats-fragments",LongType)
          .add("dropped-ether-stats-undersize-pkts",LongType)
          .add("dropped-giant-packets-greaterthan-mru",LongType)
          .add("dropped-jabbers-packets-greaterthan-mru",LongType)
          .add("dropped-miscellaneous-error-packets",LongType)
          .add("dropped-packets-with-crc-align-errors",LongType)
          .add("ether-stats-collisions",LongType)
          .add("invalid-dest-mac-drop-packets",LongType)
          .add("invalid-encap-drop-packets",LongType)
          .add("miscellaneous-output-errors",LongType)
          .add("number-of-aborted-packets-dropped",LongType)
          .add("number-of-buffer-overrun-packets-dropped",LongType)
          .add("number-of-miscellaneous-packets-dropped",LongType)
          .add("numberof-invalid-vlan-id-packets-dropped",LongType)
          .add("received-broadcast-frames",LongType)
          .add("received-good-bytes",DoubleType)
          .add("received-good-frames",LongType)
          .add("received-multicast-frames",LongType)
          .add("received-pause-frames",LongType)
          .add("received-total-bytes",LongType)
          .add("received-total-frames",LongType)
          .add("received-total-octet-frames-from1024-to1518",LongType)
          .add("received-total-octet-frames-from128-to255",LongType)
          .add("received-total-octet-frames-from1519-to-max",LongType)
          .add("received-total-octet-frames-from256-to511",LongType)
          .add("received-total-octet-frames-from512-to1023",LongType)
          .add("received-total-octet-frames-from65-to127",LongType)
          .add("received-total64-octet-frames",LongType)
          .add("received-unicast-frames",LongType)
          .add("received-unknown-opcodes",LongType)
          .add("received8021q-frames",LongType)
          .add("rfc2819-ether-stats-crc-align-errors",LongType)
          .add("rfc2819-ether-stats-jabbers",LongType)
          .add("rfc2819-ether-stats-oversized-pkts",LongType)
          .add("rfc3635dot3-stats-alignment-errors",LongType)
          .add("symbol-errors",LongType)
          .add("total-bytes-transmitted",LongType)
          .add("total-frames-transmitted",LongType)
          .add("total-good-bytes-transmitted",LongType)
          .add("transmitted-broadcast-frames",LongType)
          .add("transmitted-good-frames",LongType)
          .add("transmitted-multicast-frames",LongType)
          .add("transmitted-total-octet-frames-from1024-to1518",LongType)
          .add("transmitted-total-octet-frames-from128-to255",LongType)
          .add("transmitted-total-octet-frames-from1518-to-max",LongType)
          .add("transmitted-total-octet-frames-from256-to511",LongType)
          .add("transmitted-total-octet-frames-from512-to1023",LongType)
          .add("transmitted-total-octet-frames-from65-to127",LongType)
          .add("transmitted-total-pause-frames",LongType)
          .add("transmitted-total64-octet-frames",LongType)
          .add("transmitted-unicast-frames",LongType)
          .add("transmitted8021q-frames",LongType)
          .add("uncounted-dropped-frames",LongType))))

    //格式化时间
    val nestTimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"
    val jsonOptions =  Map("timestampFormat" -> nestTimestampFormat)


    import spark.implicits._
    //从kafka中读取数据,构造一个从主题test读取的流式DataFrame,返回的DataFrame中封装了Kafka数据记录中常见的域和相关的元数据
    val parsed = spark.readStream
      .format("kafka")  //数据来源
      //kafka参数设置
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test4")
      .option("startingOffsets", "earliest")
      .load()
      //from_json从一个json 字符串中按照指定的schema格式抽取出来作为DataFrame的列
      .select(from_json('value.cast("string"), schema) as "value").select($"value.Telemetry.collection_id")

    //通过Append的方式将结果输出到console
    val console = parsed.writeStream
      .format("console")
      .outputMode(OutputMode.Append())

    val query = console.start()

    query.awaitTermination()

  }
}
