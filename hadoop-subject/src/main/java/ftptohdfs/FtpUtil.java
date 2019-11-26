package ftptohdfs;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

/**
 * Created by Languomao on 2019/6/3.
 */
public class FtpUtil {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://10.1.1.7:8020");
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");

        Date date = new Date();
        Long time1=Calendar.getInstance().getTimeInMillis();
        //loadFromFtpToHdfs("10.1.1.121", "anonymous", "123", "/hadoop/", "/languomao/", conf);
        //loadFromHdfsToFtp("10.1.1.121", "anonymous", "123", "/hive-output/1e-test/", "/hadoop", conf);
        ftpDownFiles("10.1.1.115", "anonymous", "123", "G:\\data\\data2", "D:\\result");
        Long time2=Calendar.getInstance().getTimeInMillis();
        System.out.println("coast time:"+(time2-time1)+"ms");

    }
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
     * @author qiyongkang
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
            System.out.println("login success!");
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

            //ftp.changeWorkingDirectory(outputPath);// 转移到FTP服务器目录

            FileSystem hdfs = FileSystem.get(conf);
            List<Path> paths = new ArrayList<Path>();
            List<Path> paths2 =  getFilesUnderFolder(conf,"", filePath,paths);

            //FileStatus[] status = hdfs.listStatus(new Path(filePath));
            for (Path file : paths2) {
                String filepath = file.toString();
                String remote = outputPath + filepath.substring(20,filepath.length());
                CreateDirecroty(remote,ftp);
                ftp.changeWorkingDirectory(remote.substring(0,remote.lastIndexOf("/")+1));
                if (!(file.toString().equals(".") || file.toString().equals(".."))) {
                    inputStream = hdfs.open(new Path(file.toString())); //返回一个inputStream给ftp接收数据
                    String[] str= file.toString().split("/");
                    String filename = str[str.length-1];
                    String outputftp = remote.substring(0,remote.lastIndexOf("/")+1);
                    //System.out.println(outputftp);
                    OutputStream outputStream = ftp.storeFileStream(remote.substring(0,remote.lastIndexOf("/")+1) + filename);
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

    public static List<Path> getFilesUnderFolder(FileSystem fs, Path folderPath) throws Exception {
        List<Path> paths = new ArrayList<Path>();
        if (fs.exists(folderPath)) {
            FileStatus[] fileStatus = fs.listStatus(folderPath);
            for (int i = 0; i < fileStatus.length; i++) {
                FileStatus fileStatu = fileStatus[i];
                if (!fileStatu.isDirectory()) {//只要文件
                    Path oneFilePath = fileStatu.getPath();
                }
            }
        }
        return paths;
    }

    /**
     * ftp上传文件
     *
     * @param url
     *            FTP服务器hostname
     * @param port
     *            FTP服务器端口
     * @param username
     *            FTP登录账号
     * @param password
     *            FTP登录密码
     * @param path
     *            FTP服务器保存目录
     * @param filename
     *            上传到FTP服务器上的文件名
     * @param input
     *            输入流
     * @return 成功返回true，否则返回false
     * @author 作者
     * @date 创建时间
     * @version 1.0
     */
    public static boolean uploadFile(String url, int port, String username, String password, String path,
                                     String filename, InputStream input) throws Exception {
        boolean success = false;
        FTPClient ftp = new FTPClient();
        try {
            int reply;
            ftp.connect(url, port);// 连接FTP服务器
            // 如果采用默认端口，可以使用ftp.connect(url)的方式直接连接FTP服务器
            ftp.login(username, password);// 登录
            ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
            reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
                return success;
            }
            ftp.changeWorkingDirectory(path);
            //ftp.storeFile(filename, input);
            ftp.appendFile(filename, input);

            input.close();
            ftp.logout();
            success = true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (ftp.isConnected()) {
                try {
                    ftp.disconnect();
                } catch (IOException ioe) {
                }
            }
        }
        return success;
    }


    /**
     * 从ftp下载文件
     * @param url
     *            FTP服务器hostname
     * @param port
     *            FTP服务器端口
     * @param username
     *            FTP登录账号
     * @param password
     *            FTP登录密码
     * @param remotePath
     *            FTP服务器上的相对路径
     * @param fileName
     *            要下载的文件名
     * @param localPath
     *            下载后保存到本地的路径
     * @return
     */
    public static boolean downFile(String url, int port, String username, String password, String remotePath,
                                   String fileName, String localPath) {
        boolean success = false;
        boolean isFileExits = false;
        FTPClient ftp = new FTPClient();
        try {
            int reply;
            ftp.connect(url, port);
            // 如果采用默认端口，可以使用ftp.connect(url)的方式直接连接FTP服务器
            ftp.login(username, password);// 登录
            ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
            reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
                return success;
            }
            ftp.changeWorkingDirectory(remotePath);// 转移到FTP服务器目录
            FTPFile[] fs = ftp.listFiles();
            for (FTPFile ff : fs) {
                if (ff.getName().equals(fileName)) {
                    isFileExits=true;
                    File localFile = new File(localPath + "/" + ff.getName());
                    OutputStream is = new FileOutputStream(localFile);
                    ftp.retrieveFile(ff.getName(), is);
                    is.close();
                }
            }
            if(!isFileExits) {
                throw new RuntimeException("要下载的文件不存在！");
            }
            ftp.logout();
            success = true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (ftp.isConnected()) {
                try {
                    ftp.disconnect();
                } catch (IOException ioe) {
                }
            }
        }
        return success;
    }

    /**
     * 查询ftp服务器上指定路径所有文件名
     *
     * @param url
     *            FTP服务器hostname
     * @param port
     *            FTP服务器端口
     * @param username
     *            FTP登录账号
     * @param password
     *            FTP登录密码
     * @param remotePath
     *            FTP服务器上的相对路径
     * @return
     * @date 创建时间：2017年6月14日 下午5:06:57
     */
    public static List<String> listFTPFiles(String url, int port, String username, String password, String remotePath) {
        ArrayList<String> resultList = new ArrayList<String>();
        FTPClient ftp = new FTPClient();
        try {
            int reply;
            ftp.connect(url, port);
            // 如果采用默认端口，可以使用ftp.connect(url)的方式直接连接FTP服务器
            ftp.login(username, password);// 登录
            ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
            reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
                return resultList;
            }
            ftp.changeWorkingDirectory(remotePath);// 转移到FTP服务器目录
            FTPFile[] fs = ftp.listFiles();
            for (FTPFile ff : fs) {
                resultList.add(ff.getName());
                // if (ff.getName().equals(fileName)) {
                // File localFile = new File(localPath + "/" + ff.getName());
                // OutputStream is = new FileOutputStream(localFile);
                // ftp.retrieveFile(ff.getName(), is);
                // is.close();
                // }
            }
            ftp.logout();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (ftp.isConnected()) {
                try {
                    ftp.disconnect();
                } catch (IOException ioe) {
                }
            }
        }
        return resultList;
    }

    /**
     * 创建多层目录文件，如果有ftp服务器已存在该文件，则不创建，如果无，则创建
     * @param remote
     * @return
     * @throws Exception
     * @date 创建时间：2017年6月22日 上午11:51:33
     */
    public static boolean CreateDirecroty( String remote, FTPClient ftp) throws Exception {


        boolean success = true;
        //String directory = remote + "/";
        String directory = remote.substring(0, remote.lastIndexOf("/") + 1);
        // 如果远程目录不存在，则递归创建远程服务器目录
        if (!directory.equalsIgnoreCase("/") && !changeWorkingDirectory(new String(directory), ftp)) {
            int start = 0;
            int end = 0;
            if (directory.startsWith("/")) {
                start = 1;
            } else {
                start = 0;
            }
            end = directory.indexOf("/", start);
            String path = "";
            String paths = "";
            while (true) {

                String subDirectory = new String(remote.substring(start, end).getBytes("GBK"), "iso-8859-1");
                path = path + "/" + subDirectory;
                if (!existFile(path, ftp)) {
                    if (makeDirectory(subDirectory, ftp)) {
                        changeWorkingDirectory(path, ftp);
                    } else {
                        System.out.println("创建目录[" + subDirectory + "]失败");
                        changeWorkingDirectory(path, ftp);
                    }
                } else {
                    changeWorkingDirectory(path, ftp);
                }

                paths = paths + "/" + subDirectory;
                start = end + 1;
                end = directory.indexOf("/", start);
                // 检查所有目录是否创建完毕
                if (end <= start) {
                    break;
                }
            }
        }
        return success;
    }

    /**
     * 改变目录路径
     * @param directory
     * @param ftp
     * @return
     * @date 创建时间：2017年6月22日 上午11:52:13
     */
    public static boolean changeWorkingDirectory(String directory, FTPClient ftp) {
        boolean flag = true;
        try {
            flag = ftp.changeWorkingDirectory(directory);
            if (flag) {
                System.out.println("进入文件夹" + directory + " 成功！");
            } else {
                System.out.println("进入文件夹" + directory + " 失败！");
            }
        } catch (Exception ioe) {
            ioe.printStackTrace();
        }
        return flag;
    }

    /**
     * 创建目录
     * @param dir
     * @param ftp
     * @return
     * @date 创建时间：2017年6月22日 上午11:52:40
     */
    public static boolean makeDirectory(String dir, FTPClient ftp) {
        boolean flag = true;
        try {
            flag = ftp.makeDirectory(dir);
            if (flag) {
                System.out.println("创建文件夹" + dir + " 成功！");
            } else {
                System.out.println("创建文件夹" + dir + " 失败！");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return flag;
    }

    /**
     * 判断ftp服务器文件是否存在
     * @param path
     * @param ftp
     * @return
     * @throws Exception
     * @date 创建时间：2017年6月22日 上午11:52:52
     */
    public static boolean existFile(String path, FTPClient ftp) throws Exception {
        boolean flag = false;
        FTPFile[] ftpFileArr = ftp.listFiles(path);
        if (ftpFileArr.length > 0) {
            flag = true;
        }
        return flag;
    }

    public static List<Path> getFilesUnderFolder(Configuration conf, String Pattern, String folderPath, List<Path> paths) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(folderPath))) {
            FileStatus[] fileStatus = fs.listStatus(new Path(folderPath));
            for (int i = 0; i < fileStatus.length; i++) {
                FileStatus fileStatu = fileStatus[i];
                if (fileStatu.isFile()) {
                    Path oneFilePath = fileStatu.getPath();
                    paths.add(oneFilePath);
                }else{
                    getFilesUnderFolder(conf ,"",fileStatu.getPath().toString(),paths);
                }
            }
        }
        return paths;
    }

    public static void ftpDownFiles(String ip,String username, String password,String ftpfilepath, String localpath) throws Exception {

        FTPClient ftp = new FTPClient();

        //连接ftp
        try {
            ftp.connect(ip);
            ftp.login(username, password);
            System.out.println("连接成功...........");
            ftp.setFileType(FTP.BINARY_FILE_TYPE);
            ftp.setControlEncoding("UTF-8");
            int reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
            }

            try {

                //System.out.println(ftpfilepath);
                FTPFile[] ff = ftp.listFiles(ftpfilepath);
                // 得到当前ftp目录下的文件列表

                if (ff != null) {
                    for (int i = 0; i < ff.length; i++) {
                        //System.out.println(ff[i].getName());
                        String localfilepath = localpath + ff[i].getName();
                        File localFile = new File(localfilepath);
                        // 根据ftp文件生成相应本地文件
                        Date fflastModifiedDate = ff[i].getTimestamp().getTime();
                        // 获取ftp文件最后修改时间
                        Date localLastModifiedDate = new Date(localFile
                                .lastModified());
                        // 获取本地文件的最后修改时间
                        int result = localLastModifiedDate
                                .compareTo(fflastModifiedDate);
                        // result=0，两文件最后修改时间相同；result<0，本地文件的最后修改时间早于ftp文件最后修改时间；result>0，则相反
                        if (ff[i].isDirectory()) {
                            // 如果是目录
                            localFile.mkdir();
                            // 如果本地文件夹不存在就创建
                            String ftpfp = ftpfilepath + ff[i].getName() + "/";
                            // 转到ftp文件夹目录下
                            String localfp = localfilepath + "/";
                            // 转到本地文件夹目录下
                           ftpDownFiles(ip,username,password,ftpfp, localfp);
                            // 递归调用

                        }
                        if (ff[i].isFile()) {
                            // 如果是文件
                            File lFile = new File(localpath);
                            lFile.mkdir();
                            // 如果文件所在的文件夹不存在就创建
                            if (!lFile.exists()) {
                                return;
                            }
                            if (ff[i].getSize() != localFile.length() || result < 0) {
                                // 如果ftp文件和本地文件大小不一样或者本地文件不存在或者ftp文件有更新，就进行创建、覆盖
                                String filepath = ftpfilepath + ff[i].getName();
                                // 目标ftp文件下载路径
                                FileOutputStream fos = new FileOutputStream(
                                        localFile);
                                boolean boo;
                                try {
                                    boo = ftp.retrieveFile(new String(
                                            filepath.getBytes("UTF-8"),
                                            "ISO-8859-1"), fos);
                                    // 从FTP服务器上取回一个文件
                                } catch (Exception e) {
                                    boo = false;
                                    e.printStackTrace();
                                }

                                if (boo == true) {
                                    String name=ff[i].getName();
                                    String dir=localpath;
                                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                    String time=sdf.format(localFile.lastModified());
//                                Start test=new Start();
//                                test.getConn(name, dir, time);
                                    System.out.println(name+"   "+dir+"  "+time);
                                } else {

                                }

                                fos.flush();
                                // 将缓冲区中的数据全部写出
                                fos.close();
                                // 关闭流
                            } else {
                                //System.out.println("两个文件相同！");
                                //fos.close();
                            }
                        }

                    }
                }
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();

            }

        }catch(Exception e){
            e.printStackTrace();
        }
    }

}
