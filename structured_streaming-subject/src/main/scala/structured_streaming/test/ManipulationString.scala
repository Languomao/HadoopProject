package structured_streaming.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json

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

    //spark接收kafka数据，得到DataFrame
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    //在此处对数据进行预处理，将DataFrame中的换行与空格去除
    val data = df
      //.selectExpr("CAST(value AS STRING)")
      .writeStream
      .format("console")
      .outputMode("Append")
      .start()

      data.awaitTermination()
    //print(data)

    //使用spark-sql查询
/*    val query = df
      .selectExpr("CAST(offset AS INT)", "CAST(value AS STRING)", "CAST(topic AS STRING)", "CAST(timestamp AS timestamp)", "CAST(timestampType AS TIMESTAMP)", "CAST(partition AS STRING)", "CAST(key AS STRING)")
      .writeStream
      .format("console")
      .outputMode("Append")
      .start()

    query.awaitTermination()*/

  }
}
