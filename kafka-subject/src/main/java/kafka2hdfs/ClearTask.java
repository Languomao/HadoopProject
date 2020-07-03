package kafka2hdfs;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;

import org.apache.commons.io.FileUtils;

/**
 * Classname ClearTask
 * Date 2020/6/9 17:15
 * Created by LanKorment
 */
public class ClearTask extends TimerTask{
    //执行清理任务
    @Override
    public void run() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
        String now =sdf.format(new Date());
        //时间戳
        long time = new Date().getTime();

        File backup=new File("/data/telemetry/backup");
        File[] listFiles = backup.listFiles();
        for (File file : listFiles) {
            System.out.println(file.getAbsolutePath());

            try {
                //解析字符串的文本，生成 Date
                long pass = sdf.parse(file.getName()).getTime();
                if((time - pass) / (60*60*1000)>1){   //大于1小时删除
                    System.out.println(file.getName()+" had deleted");
                    FileUtils.deleteDirectory(file);
                }else {
                    System.out.println(file.getName()+"  deletion time not reached!");
                }

            } catch (ParseException | IOException e) {
                // TODO 自动生成的 catch 块
                e.printStackTrace();
            }
        }
    }

}
