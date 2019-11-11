package consumer;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
/**
 * Created by Languomao on 2018/6/15.
 */

public class Consumer
{
    public static String path1 = "hdfs://192.168.80.80:9000/user/languomao/input/consumer.txt";
    public static String path2 = "hdfs://192.168.80.80:9000/user/languomao/output";
    public static void main(String[] args) throws Exception
    {
        FileSystem fileSystem = FileSystem.get(new URI(path1) , new Configuration());
        if(fileSystem.exists(new Path(path2)))
        {
            fileSystem.delete(new Path(path2), true);
        }

        Job job = new Job(new Configuration(),"Consumer");
        FileInputFormat.setInputPaths(job, new Path(path1));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setNumReduceTasks(1);
        job.setPartitionerClass(HashPartitioner.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(path2));
        job.waitForCompletion(true);
        //查看执行结果
        FSDataInputStream fr = fileSystem.open(new Path("hdfs://hadoop80:9000/output/part-r-00000"));
        IOUtils.copyBytes(fr, System.out, 1024, true);
    }
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        public static long sum = 0L;
        protected void map(LongWritable k1, Text v1,Context context) throws IOException, InterruptedException
        {
            String[] splited = v1.toString().split("\t");
            if(splited[1].equals("beijing"))
            {
                sum++;
            }
        }
        protected void cleanup(Context context)throws IOException, InterruptedException
        {
            String str = "beijing";
            context.write(new Text(str),new LongWritable(sum));
        }
    }
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>
    {
        protected void reduce(Text k2, Iterable<LongWritable> v2s,Context context)throws IOException, InterruptedException
        {
            for (LongWritable v2 : v2s)
            {
                context.write(k2, v2);
            }
        }
    }
}