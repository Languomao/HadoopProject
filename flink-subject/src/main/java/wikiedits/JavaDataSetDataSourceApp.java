package wikiedits;

import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Created by Languomao on 2019/7/29.
 */
public class JavaDataSetDataSourceApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment ee = ExecutionEnvironment.getExecutionEnvironment();
        textFile(ee);
    }

    public static void textFile(ExecutionEnvironment ee) throws Exception {
        String filepath = "hdfs://hadoop6.gsta.cn:8020/user/test";
        ee.readTextFile(filepath).print();
        System.out.println("===================分割线===================");

        filepath = "hdfs://hadoop6.gsta.cn:8020/user/test/input";
        ee.readTextFile(filepath).print();
    }
}
