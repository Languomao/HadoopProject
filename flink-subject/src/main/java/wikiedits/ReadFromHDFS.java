package wikiedits;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * Created by Languomao on 2019/7/29.
 */
public class ReadFromHDFS {
    public static void main(String[] args) throws Exception {

        //可用于设置执行参数并创建从外部系统读取的源
        ExecutionEnvironment ee = ExecutionEnvironment.getExecutionEnvironment();
        //创建一个从Wikipedia IRC日志中读取的源
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        //DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());
        //读取hdfs上的文件
        DataSet<String> hdfslines = ee.readTextFile("hdfs://hadoop6.gsta.cn:8020/user/test/");
        DataStream<String> hdfslines2 = see.readTextFile("hdfs://hadoop6.gsta.cn:8020/user/test/");

        hdfslines.writeAsText("hdfs://hadoop6.gsta.cn:8020/user/flink");
        hdfslines.print();
        hdfslines2.print();

        ee.execute();
    }
}
