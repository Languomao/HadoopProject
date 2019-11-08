package utils;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Languomao on 2019/6/14.
 */

public class MRReadFromHbase extends Configured {

    public static class WCHBaseMapper extends TableMapper<Text, Text>{

        @Override
        public void map(ImmutableBytesWritable key,Result values,Context context) throws IOException, InterruptedException{
            StringBuffer sb =new StringBuffer("");
            for(Map.Entry<byte[], byte[]> value:values.getFamilyMap("content".getBytes()).entrySet()){
                String  str =new String(value.getValue());
                if(str!=null){
                    sb.append(str);
                }
                context.write(new Text(key.get()), new Text(sb.toString()));
            }
        }
    }

    public static class WCHBaseReducer extends Reducer<Text, Text, Text, Text>{
        private Text result =new Text();
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
            for(Text val:values){
                result.set(val);
                context.write(key,result);
            }
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // TODO Auto-generated method stub
        String tableName = "wordcount";
        Configuration conf =HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "szh");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        Job job =new Job(conf,"read from hbase to hdfs");
        job.setJarByClass(MRReadFromHbase.class);
        job.setReducerClass(WCHBaseReducer.class);
        TableMapReduceUtil.initTableMapperJob(tableName, new Scan(), WCHBaseMapper.class, Text.class, Text.class, job);
        FileOutputFormat.setOutputPath(job, new Path("hdfs://szh:9000/myhbase/out"));
        System.exit(job.waitForCompletion(true)?0:1);
    }

}

