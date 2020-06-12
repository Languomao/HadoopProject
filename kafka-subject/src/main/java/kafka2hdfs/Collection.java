package kafka2hdfs;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Classname Collection
 * Description TODO
 * Date 2020/6/9 17:13
 * Created by LanKorment
 */
public class Collection extends TimerTask {   //TimerTask计时器

    public static FileSystem getFileSystem(){
        FileSystem hdfs = null;
        Configuration conf = new Configuration();
        try {
            URI uri = new URI("hdfs://172.18.101.22:8020");
            hdfs = FileSystem.get(uri , conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return hdfs;
    }

    //run():计时器任务要执行的操作
    @Override
    public void run() {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
            String now = sdf.format(new Date());
            System.out.println("-------Current Time："+now+"--------");

            //创建原始数据的对象
            File srcFile = new File("/dada/telemetry/srcdata");
            File[] listFiles = srcFile.listFiles(new FilenameFilter() {     //加了一个过滤器

                //过滤器内容
                @Override
                public boolean accept(File dir, String name) {
                    //如果文件以access.log.开头，返回true,即满足if条件的留下，不满足去掉
                    if(name.startsWith("dump")){
                        return true;
                    }
                    return false;
                }
            });

            //创建待上传数据的对象
            File toUploadDir = new File("/dada/telemetry/toupload");
            for (File file : listFiles) {
                System.out.println("source files : "+file.getAbsolutePath());
                //将过滤好的文件复制到待上传的目录  copyFileToDirectory复制 moveFileToDirectory移动
                FileUtils.copyFileToDirectory(file, toUploadDir, true);         //true(如果不存在创建)
            }
            System.out.println("data had moved to dir toupload");

            //将文件上传到hdfs
            File backUpDir = new File("/dada/telemetry/backup" + now);   //上传后的文件移动到backUpDir文件夹
            File[] toUploadFiles = toUploadDir.listFiles();
            //FileSystem fs =FileSystem.get(new URI("http://172.18.101.22:9870"),new Configuration(),"root");
            FileSystem fs = getFileSystem();

            for (File file : toUploadFiles) {
                //本地上传到hdfs UUID.randomUUID生成随机数+字母
                fs.copyFromLocalFile(new Path(file.getAbsolutePath()),new Path("/kafka_logs/" + now + UUID.randomUUID()+".log"));
                //移动到待删除目录
                FileUtils.moveFileToDirectory(file, backUpDir,true);
            }
        }catch (IOException e) {
            // TODO 自动生成的 catch 块
            e.printStackTrace();
        }
    }
}
