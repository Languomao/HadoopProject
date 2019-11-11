package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Created by Languomao on 2018/8/6.
 */
public class DelectFile {

    public static void deleteFile() throws IOException {
        Configuration conf = new Configuration();
        String hdfsOutput = "hdfs://192.168.136.128:9000/user/hadoop/input/bigdata.txt";
        String hdfsPath = "hdfs://192.168.136.128:9000/";
        Path path = new Path(hdfsOutput);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
        fs.deleteOnExit(path);
        fs.close();
        System.out.println("文件已删除");
    }

    public static void main(String[] args) throws IOException {
        DelectFile df = new DelectFile();
        df.deleteFile();
    }
}
