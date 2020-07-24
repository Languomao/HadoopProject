package structured_streaming.test

import java.util.{LinkedHashMap => JLinkedHashMap}

import com.alibaba.fastjson.JSON
import com.google.gson.Gson
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Classname StreamingTest
  * Description TODO
  * Date 2020/7/17 11:41
  * Created by LanKorment
  */

  object StreamingTest extends Serializable{
    def main(args: Array[String]): Unit = {


      val conf = new SparkConf().setMaster("local[*]").setAppName("JSONDataHandler")
      val ssc = new StreamingContext(conf, Seconds(2))

      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "test",
        "auto.offset.reset" -> "earliest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )

      val topics = Array("test1")
      val stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )


          //方法一：处理JSON字符串为case class 生成RDD[case class] 然后直接转成DataFrame
          stream.map(record => handleMessage2CaseClass(record.value())).foreachRDD(rdd => {
            val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
            val df = spark.createDataFrame(rdd)
            import org.apache.spark.sql.functions._
            import spark.implicits._
            df.select(date_format($"time".cast(DateType), "yyyyMMdd").as("day"), $"*")
              //以save模式输出到目录
              .write.mode(SaveMode.Append)
              .parquet("D:\\WorkSpace\\SparktestData")
          })

      /*    //方法二：处理JSON字符串为Tuple 生成RDD[Tuple] 然后转成DataFrame
          stream.map(record => handleMessage2Tuples(record.value())).foreachRDD(rdd => {
            val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
            import spark.implicits._
            val df = rdd.toDF("id", "value", "time", "valueType", "region", "namespace")
            df.show()
          })*/

      /*    //方法三：处理JSON字符串为Row 生成RDD[Row] 然后通过schema创建DataFrame
          val schema = StructType(List(
            StructField("id", StringType),
            StructField("value", StringType),
            StructField("time", StringType),
            StructField("valueType", StringType),
            StructField("region", StringType),
            StructField("namespace", StringType))
          )
          stream.map(record => handlerMessage2Row(record.value())).foreachRDD(rdd => {
            val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
            val df = spark.createDataFrame(rdd, schema)
            df.show()
          })*/

/*      //方法四：直接将 RDD[String] 转成DataSet 然后通过schema转换
      val schema = StructType(List(
        StructField("namespace", StringType),
        StructField("id", StringType),
        StructField("region", StringType),
        StructField("time", StringType),
        StructField("value", StringType),
        StructField("valueType", StringType))
      )
      /*      val schema = new StructType()
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
              )*/

      stream.map(record => record.value()).foreachRDD(rdd => {
        val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        import org.apache.spark.sql.functions._
        val ds = spark.createDataset(rdd)
        ds.select(from_json('value.cast("string"), schema) as "value").select($"value.*").show()

        //ds.show()
      })*/

      /*    //方法五：直接将 RDD[String] 转成DataSet 然后通过read.json转成DataFrame
          stream.map(record => record.value()).foreachRDD(rdd => {
            val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
            import spark.implicits._
            val df = spark.read.json(spark.createDataset(rdd))
            df.show()
          })*/

      ssc.start()
      ssc.awaitTermination()

    }

    def handleMessage(jsonStr: String): Array[KafkaMessage] = {
      val gson = new Gson()
      gson.fromJson(jsonStr, classOf[Array[KafkaMessage]])
    }

    def handleMessage2CaseClass(jsonStr: String): KafkaMessage = {
      val gson = new Gson()
      gson.fromJson(jsonStr, classOf[KafkaMessage])
    }

    def handleMessage2Tuples(jsonStr: String): (String, String, String, String, String, String) = {
      import scala.collection.JavaConverters._
      val list = JSON.parseObject(jsonStr, classOf[JLinkedHashMap[String, Object]]).asScala.values.map(x => String.valueOf(x)).toList
      list match {
        case List(v1, v2, v3, v4, v5, v6) => (v1, v2, v3, v4, v5, v6)
      }
    }

    def handlerMessage2Row(jsonStr: String): Row = {
      import scala.collection.JavaConverters._
      val array = JSON.parseObject(jsonStr, classOf[JLinkedHashMap[String, Object]]).asScala.values.map(x => String.valueOf(x)).toArray
      Row(array: _*)
    }
  }

