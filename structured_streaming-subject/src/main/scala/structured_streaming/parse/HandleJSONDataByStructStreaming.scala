package structured_streaming.parse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_format, from_json, struct}
import org.apache.spark.sql.streaming.Trigger
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
      .master("local[2]")
      .getOrCreate()
    val source = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()
    import spark.implicits._
    val schema = StructType(List(
      StructField("id", StringType),
      StructField("value", StringType),
      StructField("time", StringType),
      StructField("valueType", StringType),
      StructField("region", StringType),
      StructField("namespace", StringType))
    )
    val data = source.select(from_json('value.cast("string"), schema) as "value").select($"value.*")
      .select(date_format($"time".cast(DateType), "yyyyMMdd").as("day"), $"*")
    val query = data
      .writeStream
      .format("parquet")
      .outputMode("Append")
      .option("checkpointLocation", "D:\\WorkSpace\\SparkTestData\\DataCheckPoint")
      .option("path", "D:\\WorkSpace\\SparktestData")
      .trigger(Trigger.ProcessingTime(3000)).partitionBy("namespace", "day")
      .start()

    query.awaitTermination()
  }
}
