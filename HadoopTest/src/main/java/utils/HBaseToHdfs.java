package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


/**
 * Created by Languomao on 2019/6/14.
 */
public class HBaseToHdfs {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, HBaseToHdfs.class.getSimpleName());
        job.setJarByClass(HBaseToHdfs.class);

        job.setMapperClass(HBaseToHdfsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        TableMapReduceUtil.initTableMapperJob(args[0], new Scan(),HBaseToHdfsMapper.class ,Text.class, Text.class, job);
        //TableMapReduceUtil.addDependencyJars(job);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }


    public static class HBaseToHdfsMapper extends TableMapper<Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper.Context context) throws IOException, InterruptedException {
            //key在这里就是hbase的rowkey
            byte[] name = null;
            byte[] age = null;
            byte[] gender = null;
            byte[] birthday = null;
            try {
                name = value.getColumnLatestCell("f1".getBytes(), "IMSI".getBytes()).getRowArray();
            } catch (Exception e) {}
            try {
                age = value.getColumnLatestCell("f1".getBytes(), "MSISDN".getBytes()).getRowArray();
            } catch (Exception e) {}
            try {
                gender = value.getColumnLatestCell("f1".getBytes(), "DestinationPort".getBytes()).getRowArray();
            } catch (Exception e) {}
            try {
                birthday = value.getColumnLatestCell("f1".getBytes(), "DestinationIP".getBytes()).getRowArray();
            } catch (Exception e) {}
            outKey.set(key.get());
            String temp = ((name==null || name.length==0)?"NULL":new String(name)) + "\t" + ((age==null || age.length==0)?"NULL":new String(age)) + "\t" + ((gender==null||gender.length==0)?"NULL":new String(gender)) + "\t" +  ((birthday==null||birthday.length==0)?"NULL":new String(birthday));
            System.out.println(temp);
            outValue.set(temp);
            context.write(outKey, outValue);
        }

    }
}
