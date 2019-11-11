package utils;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Created by Languomao on 2019/6/20.
 */
public class HBaseToHDFSMR {
    private static final String ZK_CONNECT = "hadoop1.gsta.cn:2181,hadoop2.gsta.cn:2181,hadoop3.gsta.cn:2181,hadoop6.gsta.cn,hadoop7.gsta.cn";

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZK_CONNECT);
        System.setProperty("HADOOP_USER_NAME", "hbase");
        conf.set("fs.defaultFS", "hdfs://10.1.1.6:8020");

        Job job = Job.getInstance(conf);
        job.setJarByClass(HBaseToHDFSMR.class);

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("admin"));
        /**
         * TableMapReduceUtil：以util结尾：工具
         * MapReduceFactory：以factory结尾，它是工厂类，最大作用就是管理对象的生成
         */
        TableMapReduceUtil.initTableMapperJob("test2:wid_ufdr_http", scan,
                HBaseToHDFSMRMapper.class, Text.class, NullWritable.class, job);
        job.setReducerClass(HBaseToHDFSMRReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Path outputPath = new Path("/test/hbase-output/");
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outputPath)){
            fs.delete(outputPath);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean waitForCompletion = job.waitForCompletion(true);
        System.exit(waitForCompletion ? 0 : 1);
    }

    static class HBaseToHDFSMRMapper extends TableMapper<Text, NullWritable>{
        /**
         * key:rowkey
         * value:map方法每执行一次接收到的一个参数，这个参数就是一个Result实例
         * 这个Result里面存的东西就是rowkey, family, qualifier, value, timestamp
         */
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            String rowkey = Bytes.toString(key.copyBytes());
            System.out.println(rowkey);
            List<Cell> cells = value.listCells();
            for (int i = 0; i < cells.size(); i++) {
                Cell cell = cells.get(i);
                String rowkey_result = Bytes.toString(cell.getRow()) + "\t"
                        + Bytes.toString(cell.getFamily()) + "\t"
                        + Bytes.toString(cell.getQualifier()) + "\t"
                        + Bytes.toString(cell.getValue()) + "\t"
                        + cell.getTimestamp();
                context.write(new Text(rowkey_result), NullWritable.get());
            }
        }
    }

    static class HBaseToHDFSMRReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> arg1, Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }
}
