package table;

import pojo.HttpObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import pojo.VideoObject;

/**
 * Created by Languomao on 2019/8/2.
 */

/**
 * Created by Languomao on 2019/7/30.
 */

public class DataOrderBy {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);

        DataSet<String> input = env.readTextFile("E:\\data\\http1w");
        DataSet<String> input2 = env.readTextFile("E:\\data\\video1w");
        //input.print();
        DataSet<HttpObject> httpInput = input.map(new MapFunction<String, HttpObject>() {
            @Override
            public HttpObject map(String s) throws Exception {
                String[] splits = s.split("\\|");
                return new HttpObject(splits[0],splits[1],splits[2],splits[3],splits[4],splits[5],splits[6],splits[7],splits[8],splits[9],
                        splits[10],splits[11],splits[12],splits[13],splits[14],splits[15],splits[16],splits[17],splits[18],splits[19],
                        splits[20],splits[21],splits[22],splits[23],splits[24],splits[25],splits[26],splits[27],splits[28],splits[29],
                        splits[30],splits[31],splits[32],splits[33],splits[34],splits[35],splits[36],splits[37],splits[38],splits[39],
                        splits[40],splits[41],splits[42],splits[43],splits[44],splits[45],splits[46],splits[47],splits[48],splits[49],
                        splits[50],splits[51],splits[52],splits[53],splits[54],splits[55],splits[56],splits[57],splits[58],splits[59],
                        splits[60],splits[61],splits[62],splits[63],splits[64],splits[65],splits[66],splits[67],splits[68],splits[69],
                        splits[70],splits[71],splits[72],splits[73],splits[74],splits[75],splits[76],splits[77],splits[78],splits[79],
                        splits[80],splits[81],splits[82],splits[83],splits[84],splits[85],splits[86],splits[87],splits[88],splits[89],
                        splits[90],splits[91],splits[92],splits[93],splits[94],splits[95],splits[96],splits[97],splits[98]);
            }
        });

        DataSet<VideoObject> httpInput2 = input2.map(new MapFunction<String, VideoObject>() {
            @Override
            public VideoObject map(String s) throws Exception {
                String[] splits = s.split("\\|");
                return new VideoObject(splits[0],splits[1],splits[2],splits[3],splits[4],splits[5],splits[6],splits[7],splits[8],splits[9],
                        splits[10],splits[11],splits[12],splits[13],splits[14],splits[15],splits[16],splits[17],splits[18],splits[19],
                        splits[20],splits[21],splits[22],splits[23],splits[24],splits[25],splits[26],splits[27],splits[28],splits[29],
                        splits[30],splits[31],splits[32],splits[33],splits[34],splits[35],splits[36],splits[37],splits[38],splits[39],
                        splits[40],splits[41],splits[42],splits[43],splits[44],splits[45],splits[46],splits[47],splits[48],splits[49],
                        splits[50],splits[51],splits[52],splits[53],splits[54],splits[55],splits[56],splits[57],splits[58],splits[59],
                        splits[60],splits[61],splits[62],splits[63],splits[64],splits[65],splits[66],splits[67],splits[68],splits[69],
                        splits[70],splits[71],splits[72],splits[73],splits[74],splits[75],splits[76],splits[77],splits[78],splits[79]);
            }
        });

        //将DataSet转换为Table
        Table httpTable = tableEnv.fromDataSet(httpInput);
        //注册一个表
        tableEnv.registerTable("httptable",httpTable);

        //将DataSet转换为Table
        Table httpTable2 = tableEnv.fromDataSet(httpInput2);
        //注册一个表
        tableEnv.registerTable("httptable2",httpTable2);


        //Cannot resolve field [MSISDN] given input [f0].
        Table tapiResult = tableEnv.scan("httptable").select("*");
        //tapiResult.printSchema();
        Table tapiResult2 = tableEnv.scan("httptable2").select("*");
        //tapiResult.printSchema();

        Table oederbysql = tableEnv.sqlQuery("select DestinationIP,MSISDN from httptable order by MSISDN");
        //select one
        //Table insql = tableEnv.sqlQuery("select DestinationIP,MSISDN from httptable where MSISDN='19898203723'");

        //转换回dataset
        DataSet<Result> result = tableEnv.toDataSet(oederbysql, Result.class);

        //将dataset map成tuple输出
        result.map(new MapFunction<Result, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Result result) throws Exception {
                String key = result.MSISDN;
                String value = result.DestinationIP;
                return Tuple2.of(key,value);
            }
        }).print();
        //env.execute();
    }

    public static class Result{

        public String DestinationIP;
        public String MSISDN;
        public Result() {}
    }
}
