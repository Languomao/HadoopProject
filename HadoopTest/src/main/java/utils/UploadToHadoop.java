package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;


/**
 * Created by Languomao on 2018/8/6.
 *
 * 向HDFS上传本地文件
 */
public class UploadToHadoop {

    public static void uploadInputFile(String localFile) throws IOException {
        Configuration conf = new Configuration();
        String hdfsPath = "hdfs://10.1.1.7:8020/";
        String hdfsInput = "hdfs://10.1.1.7:8020/user/test/data";   //文件上传到HDFS的位置

        //在hdfs下新建文件夹
        //Configuration conf = new Configuration();
        FileSystem file = FileSystem.get(conf);
        file.mkdirs(new Path("/user/test/data"));
        FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);  //根据给定的字符串建立URI
        for(int i = 1 ;i < 100 ;i++){
            File f = renameFile(localFile,"http1000w",i);
            String pathString = f.toString();
            System.out.println("开始上传第" + i + "个文件。。。");
            fs.copyFromLocalFile(new Path(localFile), new Path(hdfsInput));
            System.out.println("成功上传第" + i + "个文件。。。");
            System.out.println("占用空间" + i*8.08 + "G");
        }
        fs.close();
    }

    public static File renameFile(String filePath, String fileName,int count) {
        String oldFileName = filePath+"\\"+fileName;
        File oldFile = new File(oldFileName);
        String newFileName = filePath+"\\"+ fileName + "-" + count;
        File newFile = new File(newFileName);
        if (oldFile.exists() && oldFile.isFile()) {
            oldFile.renameTo(newFile);
        }
        return newFile;
    }

    public static void main(String[] args) throws IOException {
        PrintStream ps = new PrintStream("G:\\data\\data2\\Out.txt");
        System.setOut(ps);//把创建的打印输出流赋给系统。即系统下次向 ps输出
        UploadToHadoop uh = new UploadToHadoop();
        uh.uploadInputFile("G:\\data\\test");

    }
}
