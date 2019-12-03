package hadoopwrite;

import java.io.*;
import java.net.URI;
import java.net.URL;

import hadoopread.HdfsUtils;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Progressable;

/**
 * Created by Languomao on 2018/6/6.
 *
 * 将本地文件复制到HDFS指定路径的文件中（写操作）
 */
public class WriteToHadoop {

    static  {// 静态块，设置hdfs协议
        URL. setURLStreamHandlerFactory ( new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) throws IOException {

        String localsrc = "G:\\ClusterTest\\test2.txt";
        String dst = "hdfs://10.1.1.7:8020/lankorment/test.txt";

        Configuration conf = new Configuration();
        Path path = new Path(dst);
        FileSystem fs = HdfsUtils.getFileSystem();
        FSDataInputStream in = fs.open(path);//引入相应的包使用url读取数据

        try (FSDataOutputStream out = fs.create(new Path(dst),new Progressable(){
            public void progress()
            {
                System.out.print(".");//用于显示文件复制进度
            }
        }))
        {
            IOUtils.copy(in, out);
        }
    }
}
