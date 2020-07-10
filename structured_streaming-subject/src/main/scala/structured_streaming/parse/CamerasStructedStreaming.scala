package structured_streaming.parse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

/**
  * Classname CamerasStructedStreaming
  * Description TODO
  * Date 2020/7/3 11:37
  * Created by LanKorment
  */
object CamerasStructedStreaming {
  def main(args : Array[String]){

    val spark = SparkSession
      .builder()
      .appName("Structured_Streaming-Test")
      .master("local[*]")
      .getOrCreate()

    //数据预处理


    val schema = new StructType()
      .add("Source",StringType)
      .add("Telemetry",new StructType()
        .add("node_id_str",StringType)
        .add("subscription_id_str",StringType)
        .add("encoding_path",StringType)
        .add("collection_id",IntegerType)
        .add("collection_start_time",LongType)
        .add("msg_timestamp",LongType)
        .add("collection_end_time",LongType)
        )
      .add("Rows",ArrayType(new StructType()
          .add("Timestamp",LongType)
          .add("Key",new StructType()
            .add("node-name",StringType))
          .add("Content",new StructType()
              .add("process-cpu_PIPELINE_EDIT",ArrayType(new StructType()
                  .add("process-cpu-fifteen-minute",IntegerType)
                  .add("process-cpu-five-minute",IntegerType)
                  .add("process-cpu-one-minute",IntegerType)
                  .add("process-id",IntegerType)
                  .add("process-name",StringType))
                  )
              .add("total-cpu-fifteen-minute",IntegerType)
              .add("total-cpu-five-minute",IntegerType)
              .add("total-cpu-one-minute",IntegerType)
              )
            )
          )

    val nestTimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"
    val jsonOptions =  Map("timestampFormat" -> nestTimestampFormat)

    val parsed = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json(col("value").cast("string"), schema, jsonOptions).alias("parsed_value"))

    import spark.implicits._
    val camera = parsed
      .select(explode($"parsed_value.devices.cameras"))
      .select("value.*")

    camera.printSchema()
    val sightings = camera
      .select("device_id", "last_event.has_person", "last_event.start_time")

    val console = sightings.writeStream
      .format("console")
      .outputMode(OutputMode.Append())

    val query = console.start()

    query.awaitTermination()

  }
}
