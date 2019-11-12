package table;

import pojo.Student;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by Languomao on 2019/7/30.
 */
public class DataTest {
    public static void main(String[] args) throws Exception {

       /* StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);*/

        ExecutionEnvironment ee = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        DataSet<Student> set = ee.readCsvFile("E:\\data\\student.txt").fieldDelimiter(",")
                .ignoreFirstLine()
                .includeFields(true,true,true,true)
                .pojoType(Student.class,"id","name","course","score");
                set.print();

       /* DataStream<String> hdfslines = see.readTextFile("E:\\data\\data1");
        DataStream<String> hdfslines2 = see.readTextFile("E:\\data\\data2");


        Table http1 = tEnv.fromDataStream(hdfslines,"userinfo");

        //注册表
        tEnv.registerDataStream("http2", hdfslines2, "userinfo");

        //查询表
        Table result = tEnv.sqlQuery("select * from " + hdfslines + " union all select * from http2 ");

        DataStream<String> resultStream = tEnv.toAppendStream(result,String.class);

        resultStream.print();*/

        ee.execute("table");
    }

}
