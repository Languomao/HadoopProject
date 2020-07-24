package structured_streaming.data_pretreatment;

import org.apache.commons.lang.StringUtils;

import static structured_streaming.data_pretreatment.DataPretreatment.getDataStr;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Classname Pretreatment
 * Date 2020/7/17 14:53
 * Created by LanKorment
 * 预处理收集到的Telemetry数据，转换成可以使用spark解析的格式
 */
public class Pretreatment {
    public static void main(String[] args) throws Exception {
        StringBuffer str = getDataStr("D:\\WorkSpace\\Telemetry\\dump-interface.txt");
        pretreatment(data2str(str));
    }

    public static String[] data2str(StringBuffer strbuff) {
        String data = strbuff.toString();
        //System.out.println(data);
        String[] result = data.split("------- ");
        return result;
    }

    public static String replaceBlank(String str) {
        String dest = "";
        if (str != null) {
            Pattern p = Pattern.compile("\\s*|\t|\r|\n");
            Matcher m = p.matcher(str);
            dest = m.replaceAll("");
        }
        return dest;
    }

    public static void pretreatment(String[] strarr) throws Exception {

        for (int i = 1; i < strarr.length; i++) {

            String str = strarr[i].replace("-------\n", "");
            String header = StringUtils.substringBetween(str, "2020-", "]") + "]";
            String body = StringUtils.substringAfter(str, header);

            /*
            //将Header加入到Json中
            String data = replaceBlank(body);
            String json = new StringBuffer(data).insert(1,"\"Header\":" + "\"" + header + "\"" + ",").toString();*/

            //未将Header加入到body中
            String json = replaceBlank(body);

            String filePath="D:\\WorkSpace\\Telemetry\\test.txt";
            //文件追加内容
            FileOutputStream fos = new FileOutputStream(filePath,true);
            fos.write(json.getBytes());
            fos.write("\n".getBytes());

            //System.out.println(header);
            //System.out.println(json);
        }
    }
}

