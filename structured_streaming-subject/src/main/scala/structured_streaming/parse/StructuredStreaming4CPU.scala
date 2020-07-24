package structured_streaming.parse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import structured_streaming.parse.KafkaProducer.messageproducer
import structured_streaming.parse.Pretreatment.preprocessingData

/**
  * Classname StructedStreaming4CPU
  * Date 2020/7/17 15:11
  * Created by LanKorment
  * 解析CPU的数据，生成DataFrame列
  * 需要修改的参数是kafka的参数
  */
object StructuredStreaming4CPU {
  def main(args: Array[String]) {

    //预处理数据
    preprocessingData("D:\\WorkSpace\\Telemetry\\dump-cpu.txt","D:\\WorkSpace\\Telemetry\\test1.txt")

    //kafka生产消息
    messageproducer("D:\\WorkSpace\\Telemetry\\test1.txt","test1")

    //解析数据
    //新建SparkSession，设定参数
    val spark = SparkSession
      .builder
      .appName("CameliaTest")
      .master("local[*]")
      .getOrCreate()

    //根据数据格式定义schema数据结构，此处坑多，注意数据类型以及数据的层次结构，如果从kafka获取的数据是null的或者空的，先检查数据类型有没有错误，然后检查数据是否可以正常解析
    val schema = new StructType()
      .add("Header",StringType)
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
          .add("node-name",StringType))
        .add("Content",new StructType()
          .add("process-cpu_PIPELINE_EDIT",ArrayType(new StructType()
            .add("process-cpu-fifteen-minute",IntegerType)
            .add("process-cpu-five-minute",IntegerType)
            .add("process-cpu-one-minute",IntegerType)
            .add("process-id",IntegerType)
            .add("process-name",StringType)))
          .add("total-cpu-fifteen-minute",IntegerType)
          .add("total-cpu-five-minute",IntegerType)
          .add("total-cpu-one-minute",IntegerType))))

    //格式化时间
    val nestTimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"
    val jsonOptions =  Map("timestampFormat" -> nestTimestampFormat)


    import spark.implicits._
    //从kafka中读取数据,构造一个从主题test读取的流式DataFrame,返回的DataFrame中封装了Kafka数据记录中常见的域和相关的元数据
    val parsed = spark.readStream
      .format("kafka")  //数据来源
      //kafka参数设置
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test1")
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
