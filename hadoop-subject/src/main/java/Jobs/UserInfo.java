package Jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import structured_streaming.utils.UserVinfo;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Classname UserInfo
 * Description TODO
 * Date 2019/12/3 11:36
 * Created by LanKorment
 */
public class UserInfo {

    //从关注的人中获取大Ｖ及其简介,这是解析过程，获取数据
    public static String[] getUserAndInfo(String block){
        String user_regex = "<div class=\"info_name.*?<a.*?>(.*?)</a>.*?<a.*?微博个人认证.*?</a>";
        Pattern pattern1= Pattern.compile(user_regex);
        Matcher matcher1 = pattern1.matcher(block);
        String user = null;
        while(matcher1.find()){
            user = matcher1.group(1);
            if(user != null){
                String intro_regex ="<div class=\"info_intro\"><span>(.*?)</span></div>";
                Pattern pattern2= Pattern.compile(intro_regex);
                Matcher matcher2 = pattern2.matcher(block);
                if(matcher2.find()){
                    String info = matcher2.group(1);
                    String value = user + "`" + info;
                    String[] userAndInfo = value.split("`");
                    return userAndInfo;
                }
            }
        }
        return null;
    }

    //创建Mapper类和Reducer类，如何处理数据
    public static class UserVMapper extends Mapper<Object, Text, Text, Text> {

        private Text userV = new Text();
        private Text intro = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String html = value.toString();
//            String user_regex ="05050006_\">(.*?)</a>.*? title=\"微博个人" ;
//            String intro_regex ="lass=\"W_icon icon_approve\">.*?info_intro\"><span>(.*?)</span></div>";
//            String user_regex ="微博个人" ;
            String block_regex = "<li class=\"follow_item.*?</li>";
            Pattern pattern = Pattern.compile(block_regex);
            Matcher matcher = pattern.matcher(html);
            while(matcher.find()){
                String block = matcher.group();
                String[] userAndInfo = getUserAndInfo(block);
                if(userAndInfo != null){
                    String user = userAndInfo[0];
                    String info = userAndInfo[1];
                    userV.set(user);
                    intro.set(info);
                    context.write(userV, intro);
                }
            }
        }
    }

    public static class UserVReducer extends Reducer<Text, Text, Text, Text> {

        private Text value = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String result = "";
            for(Text text: values){
                String one = new String(text.toString());//之前用getByte不行
                result += one;
            }
            value.set(result);
            context.write(key, value);
        }
    }

    //执行mapReduce程序
    public static void runMapReduce(String [] args) throws Exception{
        Configuration conf = new Configuration();
        String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.err.println("Usage: wordcount<in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "UserV");
        job.setJarByClass(UserVinfo.class);
        job.setMapperClass(UserVMapper.class);
        job.setCombinerClass(UserVReducer.class);
        job.setReducerClass(UserVReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        if(job.waitForCompletion(true)==true){
            System.out.println("mapReduce 执行完毕！");
        }
//		System.exit(0);
    }

    public static void rewriteFile(String path) throws Exception{
        BufferedReader br = new BufferedReader(new FileReader(new File(path)));
        String content = "";
        String line;
        while((line = br.readLine()) != null){
            content += line;
        }
        br.close();
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(path)));
        bw.append(content);
        bw.close();
    }
}
