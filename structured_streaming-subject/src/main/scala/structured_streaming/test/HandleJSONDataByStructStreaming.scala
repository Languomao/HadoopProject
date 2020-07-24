package structured_streaming.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._

/**
  * Classname HandleJSONDataByStructStreaming
  * Description TODO
  * Date 2020/7/10 15:15
  * Created by LanKorment
  */
object HandleJSONDataByStructStreaming {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val source = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

/*    val schema = StructType(List(
      StructField("id", StringType),
      StructField("value", FloatType),
      StructField("time", StringType),
      StructField("valueType", StringType),
      StructField("region", StringType),
      StructField("namespace", StringType))
    )*/

    val schema = new StructType()
      .add("id",StringType)
      .add("value",FloatType)
      .add("time",StringType)
      .add("valueType",StringType)
      .add("region",StringType)
      .add("namespace",StringType)

    import spark.implicits._

    //将kafka中的json数据转换，然后在转换的基础上再做SQL查询
    val data = source.select(from_json('value.cast("string"), schema) as "value").select($"value.*")
        //.select("*")
      //source.select(from_json('value.cast("string"), schema) as "value").select($"value.*").show()
      //.select(date_format($"time".cast(DateType), "yyyyMMdd").as("day"), $"*")

    val query = data
      .writeStream
      .format("console")
      .outputMode("Append")
      //.option("checkpointLocation", "D:\\WorkSpace\\SparkTestData\\DataCheckPoint")
      //.option("path", "D:\\WorkSpace\\SparktestData")
      //.trigger(Trigger.ProcessingTime(3000)).partitionBy("namespace", "day")
      .start()

    query.awaitTermination()
  }
}
