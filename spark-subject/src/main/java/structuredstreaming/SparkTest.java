package structuredstreaming;

import org.apache.spark.SparkConf;

/**
 * Classname SparkTest
 * Date 2020/7/1 17:46
 * Created by LanKorment
 */
public class SparkTest {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        //JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
    }
}
