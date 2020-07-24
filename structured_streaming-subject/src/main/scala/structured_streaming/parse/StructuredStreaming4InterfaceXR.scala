package structured_streaming.parse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import structured_streaming.parse.KafkaProducer.messageproducer
import structured_streaming.parse.Pretreatment.preprocessingData

/**
  * Classname StructuredStreaming4InterfaceXR
  * Date 2020/7/17 15:45
  * Created by LanKorment
  * 解析InterfaceXR的数据，生成DataFrame列
  * 需要修改的参数是kafka的参数
  */
object StructuredStreaming4InterfaceXR {
  def main(args: Array[String]) {

    preprocessingData("D:\\WorkSpace\\Telemetry\\dump-interface-xr.txt","D:\\WorkSpace\\Telemetry\\test2.txt")

    //kafka生产消息
    messageproducer("D:\\WorkSpace\\Telemetry\\test2.txt","test2")
    //新建SparkSession，设定参数
    val spark = SparkSession
      .builder
      .appName("CameliaTest")
      .master("local[*]")
      .getOrCreate()

    //根据数据格式定义schema数据结构，此处坑多，注意数据类型以及数据的层次结构，如果从kafka获取的数据是null的或者空的，先检查数据类型有没有错误，然后检查数据是否可以正常解析
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
          .add("arp-information",new StructType()
            .add("arp-is-learning-disabled",StringType)
            .add("arp-timeout",IntegerType)
            .add("arp-type-name",StringType))
          .add("bandwidth",IntegerType)
          .add("burned-in-address",new StructType()
            .add("address",StringType))
          .add("carrier-delay",new StructType()
            .add("carrier-delay-down",IntegerType)
            .add("carrier-delay-up",IntegerType))
          .add("data-rates",new StructType()
            .add("bandwidth",IntegerType)
            .add("input-data-rate",IntegerType)
            .add("input-load",IntegerType)
            .add("input-packet-rate",IntegerType)
            .add("load-interval",IntegerType)
            .add("output-data-rate",IntegerType)
            .add("output-load",IntegerType)
            .add("output-packet-rate",IntegerType)
            .add("peak-input-data-rate",IntegerType)
            .add("peak-input-packet-rate",IntegerType)
            .add("peak-output-data-rate",IntegerType)
            .add("peak-output-packet-rate",IntegerType)
            .add("reliability",IntegerType))
          .add("description",StringType)
          .add("duplexity",StringType)
          .add("encapsulation",StringType)
          .add("encapsulation-type-string",StringType)
          .add("fast-shutdown",StringType)
          .add("hardware-type-string",StringType)
          .add("if-index",IntegerType)
          .add("in-flow-control",StringType)
          .add("interface-handle",StringType)
          .add("interface-statistics",new StructType()
            .add("full-interface-stats",new StructType()
              .add("applique",IntegerType)
              .add("availability-flag",IntegerType)
              .add("broadcast-packets-received",IntegerType)
              .add("broadcast-packets-sent",IntegerType)
              .add("bytes-received",DoubleType)
              .add("bytes-sent",DoubleType)
              .add("carrier-transitions",IntegerType)
              .add("crc-errors",IntegerType)
              .add("framing-errors-received",IntegerType)
              .add("giant-packets-received",IntegerType)
              .add("input-aborts",IntegerType)
              .add("input-drops",IntegerType)
              .add("input-errors",IntegerType)
              .add("input-ignored-packets",IntegerType)
              .add("input-overruns",IntegerType)
              .add("input-queue-drops",IntegerType)
              .add("last-data-time",TimestampType)
              .add("last-discontinuity-time",TimestampType)
              .add("multicast-packets-received",IntegerType)
              .add("multicast-packets-sent",IntegerType)
              .add("output-buffer-failures",IntegerType)
              .add("output-buffers-swapped-out",IntegerType)
              .add("output-drops",IntegerType)
              .add("output-errors",IntegerType)
              .add("output-queue-drops",IntegerType)
              .add("output-underruns",IntegerType)
              .add("packets-received",IntegerType)
              .add("packets-sent",IntegerType)
              .add("parity-packets-received",IntegerType)
              .add("resets",IntegerType)
              .add("runt-packets-received",IntegerType)
              .add("seconds-since-last-clear-counters",IntegerType)
              .add("seconds-since-packet-received",IntegerType)
              .add("seconds-since-packet-sent",IntegerType)
              .add("throttled-packets-received",IntegerType)
              .add("unknown-protocol-packets-received",IntegerType))
            .add("stats-type",StringType))
          .add("interface-type",StringType)
          .add("ip-information",new StructType()
            .add("ip-address",StringType)
            .add("subnet-mask-length",IntegerType))
          .add("is-dampening-enabled",StringType)
          .add("is-l2-looped",StringType)
          .add("is-l2-transport-enabled",StringType)
          .add("last-state-transition-time",StringType)
          .add("line-state",StringType)
          .add("link-type",StringType)
          .add("mac-address",new StructType()
            .add("address",StringType))
          .add("max-bandwidth",IntegerType)
          .add("media-type",StringType)
          .add("mtu",IntegerType)
          .add("out-flow-control",StringType)
          .add("parent-interface-name",StringType)
          .add("speed",IntegerType)
          .add("state",StringType)
          .add("state-transition-count",IntegerType))))

    //格式化时间
    val nestTimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"
    val jsonOptions =  Map("timestampFormat" -> nestTimestampFormat)


    import spark.implicits._
    //从kafka中读取数据,构造一个从主题test读取的流式DataFrame,返回的DataFrame中封装了Kafka数据记录中常见的域和相关的元数据
    val parsed = spark.readStream
      .format("kafka")  //数据来源
      //kafka参数设置
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test2")
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
