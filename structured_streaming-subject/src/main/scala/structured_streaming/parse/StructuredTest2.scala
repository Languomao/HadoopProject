package structured_streaming.parse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_format, from_json, struct}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, _}

/**
  * Classname StructuredTest2
  * Description TODO
  * Date 2020/7/15 14:37
  * Created by LanKorment
  */
object StructuredTest2 {
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
      .option("subscribe", "test3")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    import spark.implicits._

    //schema定义过程中要根据json数据来选定结构以及数据类型
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
        .add("Content",MapType(StringType,new StructType()
          .add("arp-is-learning-disabled",StringType)
          .add("arp-timeout",IntegerType)
          .add("arp-type-name",StringType)))))

    source.printSchema()

    val data = source.select(from_json('value.cast("string"), schema) as "value").select($"value.*")

    val query = data
      .writeStream
      .format("console")
      .outputMode("Append")
      //.option("checkpointLocation", "D:\\WorkSpace\\SparkTestData\\DataCheckPoint")
      //.option("path", "D:\\WorkSpace\\SparkTestData")
      //.trigger(Trigger.ProcessingTime(3000)).partitionBy("namespace", "day")
      .start()

    query.awaitTermination()
  }

  /*def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

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
        .add("Content",MapType(StringType,new StructType()
          .add("arp-is-learning-disabled",StringType)
          .add("arp-timeout",IntegerType)
          .add("arp-type-name",StringType)))))


    val source = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test8")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    import spark.implicits._

    stream.map(record => record.value()).foreachRDD(rdd => {
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val ds = spark.createDataset(rdd)
      import org.apache.spark.sql.functions._
      val df = ds.select(from_json('value, ArrayType(schema)) as "value").select(explode('value)).select($"col.*")
      df.show()
    }
    //data就是将kafka中的json数据转换后的数据，然后在转换的基础上再做SQL查询
    val data = source.select(from_json('value.cast("string"), schema) as "value").select($"value.*")
      .select("*")
    //source.select(from_json('value.cast("string"), schema) as "value").select($"value.*").show()
    //.select(date_format($"time".cast(DateType), "yyyyMMdd").as("day"), $"*")

    val query = data
      .writeStream
      .format("console")
      .outputMode("Append")

      .start()

    query.awaitTermination()
  }*/
}
