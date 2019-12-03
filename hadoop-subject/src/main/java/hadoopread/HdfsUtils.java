package hadoopread;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

/**
 * Created by Languomao on 2018/6/6.
 */
public class HdfsUtils {

    public static FileSystem getFileSystem(){
        FileSystem hdfs = null;
        Configuration conf = new Configuration();
        try {
            URI uri = new URI("hdfs://10.1.1.7:8020");
            hdfs = FileSystem.get(uri , conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return hdfs;
    }
}
