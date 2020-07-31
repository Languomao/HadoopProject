package structured_streaming.test


import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Classname Demo03
  * Description TODO
  * Date 2020/7/27 17:24
  * Created by LanKorment
  */

object Demo03 {
  private val logger = LoggerFactory.getLogger(Demo03.getClass)

  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    import session.implicits._
    val frame = session.read.json("D:\\WorkSpace\\test.txt")


    frame.createOrReplaceTempView("tables01")

    val sql_a =
      """
        |select * from tables01
      """.stripMargin

    val df1 = session.sql(sql_a)
    // df1.foreach(t=> println(t))
    println("-------------111111111---------------------")
    val rdd = df1.rdd.flatMap {
      row =>
        val dataId = row.getAs[Long]("dataId")
        val dataType = row.getAs[String]("dataType")
        println(dataType.toString + "-------------2222222222222222---------------------")

        row.getAs[Seq[Row]]("resultData").map {
          rows =>
            val binlog = rows.getAs[String]("binlog")
            var tag = ""
            rows.getAs[Seq[Row]]("column").map {
              row02 =>
                val columnname = row02.getAs[String]("columnname")
                val columntype = row02.getAs[String]("columntype")
                val index = row02.getAs[Long]("index")
                val modified = row02.getAs[Boolean]("modified")
                val pk = row02.getAs[Boolean]("pk")
                val sqlType = row02.getAs[Long]("sqlType")
                val value = row02.getAs[String]("value")

                tag = String.format(s"[columnname:$columnname,columntype:$columntype,index:$index,modified:$modified,pk:$pk,sqlType：$sqlType,value:$value]")
            }

            val db = rows.getAs[String]("db")
            val eventType = rows.getAs[String]("eventType")
            val pkValue = rows.getAs[String]("pkValue")
            val sql = rows.getAs[String]("sql")
            val table = rows.getAs[String]("table")
            val time = rows.getAs[Long]("time")

            val tags = String.format(s"[dataId:$dataId,dataType:$dataType,binlog:$binlog,tag:$tag,db:$db,eventType:$eventType,pkValue:$pkValue,sql:$sql,table:$table,time:$time]")

            //dataId, dataType, binlog, tag, db, eventType, pkValue, sql, table, time,
            ( tags)

        }
    }

    rdd.toDF().write.saveAsTable("tmp_table3")

    //,_2,_3,_4,_5,_6,_7,_8,_9,_10
    val result =
      """
        |select * from tmp_table3
      """.stripMargin
    session.sql(result).write.json("D:\\WorkSpace\\output")
  }
}

