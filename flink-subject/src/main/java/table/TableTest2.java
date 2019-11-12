package table;

import pojo.TopScorers;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * Created by Languomao on 2019/7/31.
 */
public class TableTest2 {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);

        //DataSet<String> input = env.readTextFile("/home/test/data2.csv");
        DataSet<String> input = env.readTextFile("E:\\data\\data2.csv");
        input.print();
        DataSet<TopScorers> topInput = input.map(new MapFunction<String, TopScorers>() {
            @Override
            public TopScorers map(String s) throws Exception {
                String[] splits = s.split(",");
                return new TopScorers(Integer.valueOf(splits[0]),splits[1],splits[2],Integer.valueOf(splits[3]),Integer.valueOf(splits[4]),Integer.valueOf(splits[5]),Integer.valueOf(splits[6]),Integer.valueOf(splits[7]),Integer.valueOf(splits[8]),Integer.valueOf(splits[9]),Integer.valueOf(splits[10]));
            }
        });

        //将DataSet转换为Table
        Table topScore = tableEnv.fromDataSet(topInput);
        //将topScore注册为一个表
        tableEnv.registerTable("topScore",topScore);

        Table tapiResult = tableEnv.scan("topScore").select("*");
        tapiResult.printSchema();

        Table groupedByCountry = tableEnv.sqlQuery("select player,sum(jinqiu) as sum_score from topScore group by player");

        //转换回dataset
        DataSet<Result> result = tableEnv.toDataSet(groupedByCountry, Result.class);

        //将dataset map成tuple输出
        result.map(new MapFunction<Result, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Result result) throws Exception {
                String player = result.player;
                int sum_score = result.sum_score;
                return Tuple2.of(player,sum_score);
            }
        }).print();

        env.execute("test");
    }

    /**
     * 统计结果对应的类
     */
    public static class Result {
        public String player;
        public Integer sum_score;

        public Result() {}
    }
}
