package structured_streaming.test

import org.apache.spark.sql.SparkSession

/**
  * Classname ManipulationString
  * Description TODO
  * Date 2020/7/31 14:51
  * Created by LanKorment
  */
object ManipulationString {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val query = df
      .selectExpr("CAST(offset AS INT)", "CAST(value AS STRING)", "CAST(topic AS STRING)", "CAST(timestamp AS timestamp)", "CAST(timestampType AS STRING)", "CAST(partition AS STRING)", "CAST(key AS STRING)")
      .writeStream
      .format("console")
      .outputMode("Append")
      .start()

    query.awaitTermination()

  }
}
