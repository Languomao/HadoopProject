package structured_streaming.test

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{StringType, _}
import structured_streaming.parse.KafkaProducer.messageproducer
import structured_streaming.parse.Pretreatment.preprocessingData

/**
  * Classname StructuredTest2
  * Description TODO
  * Date 2020/7/15 14:37
  * Created by LanKorment
  */
object StructuredTest2 {
  def main(args: Array[String]): Unit = {

    preprocessingData("D:\\WorkSpace\\Telemetry\\dump-topology-summary.txt","D:\\WorkSpace\\Telemetry\\test5.txt")

    //kafka生产消息
    messageproducer("D:\\WorkSpace\\Telemetry\\test5.txt","test5")

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    //创建Spark SQL的切入点（RDD的切入点是SparkContext）
    // SparkSession封装了SparkConf、SparkContext和SQLContext，SparkSession实质上是SQLContext和HiveContext的组合
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      //允许使用HQL
      .enableHiveSupport()
      .master("local[*]")
      .config("spark.hadoop.hive.metastore.uris","thrift://localhost:9083")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .getOrCreate()

    //val sc: SparkContext = spark.sparkContext
    //生成DataFrame数据集，可以把DataFrame看作是一张不断追加的表
    val source = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test5")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    //schema定义过程中要根据json数据来选定结构以及数据类型
    val schema = new StructType()
      .add("header",StringType)
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
        .add("Keys",StringType)
        .add("adjacency-sids",IntegerType)
        .add("links",IntegerType)
        .add("nodes",IntegerType)
        .add("prefix-sids",IntegerType)
        .add("prefixes",IntegerType)))

    import spark.implicits._

    val data = source.select(from_json('value.cast("string"), schema) as "value")
      //.toDF("header","Source","Telemetry")
      .select($"value.Source", $"value.Telemetry.node_id_str",$"value.Telemetry.subscription_id_str",$"value.Telemetry.encoding_path",
        $"value.Telemetry.collection_id",$"value.Telemetry.collection_start_time",$"value.Telemetry.msg_timestamp",$"value.Telemetry.collection_end_time",
        $"value.Rows.Timestamp",$"value.Rows.Keys",$"value.Rows.adjacency-sids",$"value.Rows.links",$"value.Rows.nodes",$"value.Rows.prefix-sids",$"value.Rows.prefixes")
      //注册一个名为topology的临时视图，另外还有一个方法registerTempTable(tableName)，但此方法已经过时
      //.createOrReplaceTempView("topology")

    //流式数据不能使用show来输出，应该使用writeStream,Queries with streaming sources must be executed with writeStream.start();
    //data.show(100)

    //通过Spark-SQl向hive中插入数据
    //sql("use test")
    //将从topology表的查询结果插入到topology_table表
    //sql("insert into table_topology values('header', 'source', 'node_id_str', 'subscription_id_str', 'encoding_path', 1, 'collection_start_time', 'msg_timestamp', 'collection_end_time', 'row_timestamp', 'keys', 1, 1, 1, 1, 1)")
    //sql("insert into table_topology select * from topology")

    val query = data
      .writeStream
      //can be "orc", "json", "csv", etc.
      .format("json")
      .option("checkpointLocation", "hdfs://localhost:9000/test/spark/checkpoint")
      .option("path", "hdfs://localhost:9000/test/spark/data")
      .outputMode("Append")
      .start()

    query.awaitTermination()

  }
}
