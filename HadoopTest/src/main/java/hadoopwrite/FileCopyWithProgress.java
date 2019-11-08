package hadoopwrite;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

/**
 * Created by Languomao on 2018/6/6.
 *
 * 将本地文件复制到HDFS指定路径的文件中（写操作）
 */
public class FileCopyWithProgress {

    static  {// 静态块，设置hdfs协议
        URL. setURLStreamHandlerFactory ( new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) throws IOException {

        //String localsrc = "hdfs://localhost:9000/user/languomao/input/test.txt";
        //String dst = "hdfs://localhost:9000/user/languomao/output/inputtest.txt";
        //String localsrc = args[0];
        //String dst = args[1];

        Configuration conf = new Configuration();
        URL url = new URL(args[0]);
        InputStream in = url.openStream();//引入相应的包使用url读取数据
        FileSystem fs = FileSystem.get(URI.create(args[1]),conf);

        try (OutputStream out = fs.create(new Path(args[1]),new Progressable(){
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
