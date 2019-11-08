package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Languomao on 2018/5/25.
 */
public class WordCountJobSubmitter {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        Job wordCountJob = Job.getInstance(conf);

        //重要：指定本job所在的jar包
        wordCountJob.setJarByClass(WordCountJobSubmitter.class);

        //设置wordCountJob所用的mapper逻辑类为哪个类
        wordCountJob.setMapperClass(WordCountMapper.class);
        //设置wordCountJob所用的reducer逻辑类为哪个类
        wordCountJob.setReducerClass(WordCountReducer.class);

        //设置map阶段输出的kv数据类型
        wordCountJob.setMapOutputKeyClass(Text.class);
        wordCountJob.setMapOutputValueClass(IntWritable.class);

        //设置最终输出的kv数据类型
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);

        //设置要处理的文本数据所存放的路径
        FileInputFormat.setInputPaths(wordCountJob, "hdfs://localhost:9000/user/languomao/input/test.txt");
        FileOutputFormat.setOutputPath(wordCountJob, new Path("hdfs://localhost:9000/user/languomao/output"));


        //提交job给hadoop集群
        wordCountJob.waitForCompletion(true);
    }
}