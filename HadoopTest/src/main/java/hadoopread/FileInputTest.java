package hadoopread;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;

/**
 * Created by Languomao on 2018/6/6.
 *
 * 读取HDFS指定路径的文件内容（读操作）
 */
public class FileInputTest {

    public static void main(String[] args) throws IOException {

        FileSystem hdfs = HdfsUtils.getFileSystem();
        FSDataInputStream fsDataInputStream = hdfs.open(new Path("hdfs://localhost:9000/user/languomao/input/BigDataModule.jar"));
        IOUtils.copyBytes(fsDataInputStream , System.out, 4096,false);
    }
}
