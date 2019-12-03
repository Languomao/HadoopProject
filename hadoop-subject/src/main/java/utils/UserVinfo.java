package utils;


import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by Languomao on 2018/8/6.
 *
 * 删除HDFS的文件
 */

public class UserVinfo {

    public static void main(String[] args) throws Exception{
        /*String localFile = "/home/guomao/Mapreduce测试";
        String downloadFile = "/home/guomao/result";
        File[] dir = new File(localFile).listFiles();
        for(File file:dir){
            System.out.println(file.getName());
            String inputFile = localFile + "/" + file.getName();
            File[] input = new File(inputFile).listFiles();
        }*/

        //getUserAndInfo("languomao");
        System.out.print("报告短短，一切正常进行完毕！");
    }

    //向HDFS上传本地文件
    public static void uploadInputFile(String localFile) throws IOException{
        Configuration conf = new Configuration();
        String hdfsPath = "hdfs://localhost:9000/";
        String hdfsInput = "hdfs://localhost:9000/user/hadoop/input";
        //在hdfs下新建文件夹
//		Configuration conf = new Configuration();
        FileSystem file = FileSystem.get(conf);
        file.mkdirs(new Path("/user/hadoop/input"));

        FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
        fs.copyFromLocalFile(new Path(localFile), new Path(hdfsInput));
        fs.close();
        System.out.println("已经上传到input文件夹啦");
    }

    //创建文件夹（不适用于hdfs即hadoop的文件系统）
//	public static boolean mkDirectory(String path) {
//        File file = null;
//        try {
//            file = new File(path);
//            if (!file .exists()  && !file .isDirectory()) {
//                return file.mkdirs();
//            }
//            else{
//                return false;
//            }
//        } catch (Exception e) {
//        } finally {
//            file = null;
//        }
//        return false;
//    }
//
//将output文件下载到本地
    public static void getOutput(String outputfile) throws IOException{
        String remoteFile = "hdfs://localhost:9000/user/hadoop/output/part-r-00000";
        File file=new File(remoteFile);
        if(file.exists())
        {
            file.renameTo(new File(outputfile));
        }
        Path path = new Path(remoteFile);
        Configuration conf = new Configuration();
        String hdfsPath = "hdfs://localhost:9000/";
        FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
        fs.copyToLocalFile(path, new Path(outputfile));
        System.out.println("已经将文件保留到本地文件");
        fs.close();
    }

    //删除HDFS中的output文件
    public static void deleteOutput() throws IOException{
        Configuration conf = new Configuration();
        String hdfsOutput = "hdfs://10.1.1.7:8020//lankorment/age.txt";
        String hdfsPath = "hdfs://10.1.1.7:8020/";
        Path path = new Path(hdfsOutput);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
        fs.deleteOnExit(path);
        fs.close();
        System.out.println("文件已删除");
    }

    //删除HDFS中的input文件
    public static void deleteInput() throws IOException{
        Configuration conf = new Configuration();
        String hdfsInput = "hdfs://localhost:9000/user/hadoop/input";
        String hdfsPath = "hdfs://localhost:9000/";
        Path path = new Path(hdfsInput);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
        fs.deleteOnExit(path);
        fs.close();
        System.out.println("文件已删除");
    }

   /*public static void main(String[] args) throws Exception{
        String localFile = "/home/guomao/Mapreduce测试";
        String downloadFile = "/home/guomao/result";
        File[] dir = new File(localFile).listFiles();
        for(File file:dir){
            System.out.println(file.getName());
            String inputFile = localFile + "/" + file.getName();
            File[] input = new File(inputFile).listFiles();
            for(File txt: input){
                String txtPath = inputFile + "/" + txt.getName();
                rewriteFile(txtPath);
                //上传文件至input文件夹中
                uploadInputFile(txtPath);
            }
            //进行mapreduce操作
            runMapReduce(args);
            //将output文件进行改名字并下载
            getOutput(downloadFile+"/" + file.getName()+".txt");
            //将output文件夹删除
            deleteOutput();
            //将input文件夹删除
            deleteInput();
//			 break;
        }
        System.out.print("报告短短，一切正常进行完毕！");
    }*/
}
