package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * Created by Languomao on 2018/8/6.
 *
 * 将HDFS的文件下载到本地
*/
public class DownloadFromHadoop {

    public static void getOutput(String outputfile) throws IOException {
        String remoteFile = "hdfs://192.168.136.128:9000/user/hadoop/input/bigdata.txt";
        File file=new File(remoteFile);
        if(file.exists())
        {
            file.renameTo(new File(outputfile));
        }
        Path path = new Path(remoteFile);
        Configuration conf = new Configuration();
        String hdfsPath = "hdfs://192.168.136.128:9000/";
        FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
        fs.copyToLocalFile(path, new Path(outputfile));
        System.out.println("已经将文件保留到本地文件");
        fs.close();
    }

    public static void main(String[] args) throws IOException {
        DownloadFromHadoop dfh = new DownloadFromHadoop();
        dfh.getOutput("E:/Hadoop/Data");
    }
}
