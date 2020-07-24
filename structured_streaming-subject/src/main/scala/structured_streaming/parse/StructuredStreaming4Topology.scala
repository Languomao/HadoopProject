package structured_streaming.parse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import structured_streaming.parse.KafkaProducer.messageproducer
import structured_streaming.parse.Pretreatment.preprocessingData

/**
  * Classname StructuredStreaming4Topology
  * Description TODO
  * Date 2020/7/21 16:06
  * Created by LanKorment
  */
object StructuredStreaming4Topology {
  def main(args: Array[String]) {

    preprocessingData("D:\\WorkSpace\\Telemetry\\dump-topology-summary.txt","D:\\WorkSpace\\Telemetry\\test5.txt")

    //kafka生产消息
    messageproducer("D:\\WorkSpace\\Telemetry\\test5.txt","test5")

    //新建SparkSession，设定参数
    val spark = SparkSession
      .builder
      .appName("CameliaTest")
      .master("local[*]")
      .getOrCreate()

    //根据数据格式定义schema数据结构，此处坑多，注意数据类型以及数据的层次结构
    // 如果从kafka获取的数据是null的或者空的，先检查数据类型有没有错误,只要有一个值的类型不匹配，获得的值都会为null，然后检查数据是否可以正常解析
    val schema = new StructType()
      //.add("Header",StringType)
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
        .add("adjacency-sids",IntegerType)
        .add("links",IntegerType)
        .add("nodes",IntegerType)
        .add("prefix-sids",IntegerType)
        .add("prefixes",IntegerType)))

    //格式化时间
    /*val nestTimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"
    val jsonOptions =  Map("timestampFormat" -> nestTimestampFormat)*/

    import spark.implicits._
    //从kafka中读取数据,构造一个从主题test读取的流式DataFrame,返回的DataFrame中封装了Kafka数据记录中常见的域和相关的元数据
    val parsed = spark.readStream
      .format("kafka")  //数据来源
      //kafka参数设置
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test5")
      .option("startingOffsets", "earliest")
      .load()
      //from_json从一个json 字符串中按照指定的schema格式抽取出来作为DataFrame的列
      .select(from_json('value.cast("string"), schema) as "value").select($"value.Source",
      $"value.Telemetry.node_id_str",$"value.Telemetry.subscription_id_str",$"value.Telemetry.encoding_path",$"value.Telemetry.collection_id",$"value.Telemetry.collection_start_time",$"value.Telemetry.msg_timestamp",$"value.Telemetry.collection_end_time",
      $"value.Rows.adjacency-sids",$"value.Rows.links",$"value.Rows.nodes",$"value.Rows.prefix-sids",$"value.Rows.prefixes")

    /*val df = spark.readStream
      .format("kafka")  //数据来源
      //kafka参数设置
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test5")
      .option("startingOffsets", "earliest")
      .load()*/

/*    val ds = parsed
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .select(from_json('value.cast("string"), schema) as "value")
      .select($"value.Source",
      $"value.Telemetry.node_id_str",$"value.Telemetry.subscription_id_str",$"value.Telemetry.encoding_path",$"value.Telemetry.collection_id",$"value.Telemetry.collection_start_time",$"value.Telemetry.msg_timestamp",$"value.Telemetry.collection_end_time",
      $"value.Rows.adjacency-sids",$"value.Rows.links",$"value.Rows.nodes",$"value.Rows.prefix-sids",$"value.Rows.prefixes")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "testout")
      .option("checkpointLocation", "E:\\Kafka\\checkpoint")
      .start()*/


    //通过Append的模式将结果输出到console
    val console = parsed.writeStream
      .format("console")
      .outputMode(OutputMode.Append())

    //通过Append的模式将结果输出到kafka
/*
    val console = parsed.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "testout")
      .option("checkpointLocation", "E:\\Kafka\\checkpoint")
      .outputMode("append")
      .start()
*/

    //val query = console.start()

    //query.awaitTermination()

  }
}
