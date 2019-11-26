package ftp;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
/**
 * Classname FtpUtil
 * Description TODO
 * Date 2019/11/26 10:50
 * Created by LanKorment
 */

public class FtpUtil{
    /**
     * loadFromFtpToHdfs:将数据从ftp上传到hdfs上. <br/>
     *
     * @param ip
     * @param username
     * @param password
     * @param filePath
     * @param outputPath
     * @param conf
     * @return
     * @author
     * @since JDK 1.8
     */
    public static boolean loadFromFtpToHdfs(String ip, String username, String password, String filePath, String outputPath, Configuration conf) {
        FTPClient ftp = new FTPClient();
        InputStream inputStream = null;
        FSDataOutputStream outputStream = null;
        boolean flag = true;
        try {
            ftp.connect(ip);
            ftp.login(username, password);
            System.out.println("login success!");
            ftp.setFileType(FTP.BINARY_FILE_TYPE);
            ftp.setControlEncoding("UTF-8");
            int reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
            }
            FTPFile[] files = ftp.listFiles(filePath);
            FileSystem hdfs = FileSystem.get(conf);
            for (FTPFile file : files) {
                if (!(file.getName().equals(".") || file.getName().equals(".."))) {
                    inputStream = ftp.retrieveFileStream(filePath + file.getName());
                    outputStream = hdfs.create(new Path(outputPath + file.getName()));
                    IOUtils.copyBytes(inputStream, outputStream, conf, false);
                    if (inputStream != null) {
                        inputStream.close();
                        ftp.completePendingCommand();
                    }
                }
            }
            ftp.disconnect();
        } catch (Exception e) {
            flag = false;
            e.printStackTrace();
        }
        return flag;
    }

    private static boolean loadFromHdfsToFtp(String ip, String username, String password, String filePath, String outputPath, Configuration conf) {
        FTPClient ftp = new FTPClient();
        FSDataInputStream inputStream = null;

        //连接ftp
        boolean flag = true;
        try {
            ftp.connect(ip);
            ftp.login(username, password);
            //System.out.println("login success!");
            boolean flag1 = ftp.changeWorkingDirectory(outputPath);
            if(!flag1){
                ftp.makeDirectory(outputPath);
            }
            ftp.changeWorkingDirectory(outputPath);
            ftp.setFileType(FTP.BINARY_FILE_TYPE);
            ftp.setControlEncoding("UTF-8");
            int reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
            }

            //FTPFile[] files = ftp.listFiles(filePath);
            FileSystem hdfs = FileSystem.get(conf);
            FileStatus[] status = hdfs.listStatus(new Path(filePath));
            for (FileStatus file : status) {
                if (!(file.getPath().equals(".") || file.getPath().equals(".."))) {
                    inputStream = hdfs.open(file.getPath()); //返回一个inputStream给ftp接收数据
                    String[] str= file.getPath().toString().split("/");
                    String filename = str[str.length-1];
                    OutputStream outputStream = ftp.storeFileStream(outputPath + filename);
                    IOUtils.copyBytes(inputStream, outputStream, conf, false);
                    if (outputStream != null) {
                        outputStream.close();
                        ftp.completePendingCommand();
                    }
                }
            }

            ftp.disconnect();
        } catch (Exception e) {
            flag = false;
            e.printStackTrace();
        }
        return flag;
    }

    public static  void main(String... args) {
        String arg0= args[0];
        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://10.1.1.227:8020");
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");

        Date date = new Date();

        if("loadFromFtpToHdfs".equalsIgnoreCase(arg0)){

            Long time1= Calendar.getInstance().getTimeInMillis();
            System.out.println("data begin uploading...");

            loadFromFtpToHdfs(args[1],args[2],args[3],args[4],args[5],conf);

            Long time2=Calendar.getInstance().getTimeInMillis();

            System.out.println("spend time:"+(time2-time1)+"ms");
            System.out.println("data uploaded!");
        }else if("loadFromHdfsToFtp".equalsIgnoreCase(arg0)){

            Long time1= Calendar.getInstance().getTimeInMillis();
            System.out.println("data begin downloading...");

            loadFromHdfsToFtp(args[1],args[2],args[3],args[4],args[5],conf);

            Long time2=Calendar.getInstance().getTimeInMillis();

            System.out.println("spend time:"+(time2-time1)+"ms");
            System.out.println("data downloaded!");
        }else{
            System.out.println("something wrong!");
        }
    }


}

