package structured_streaming.data_pretreatment;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Classname DataPretreatment
 * Date 2020/6/16 10:51
 * Created by LanKorment
 */
public class DataPretreatment {

    public static void main(String[] args){
        StringBuffer strbuff = getDataStr("D:\\WorkSpace\\Telemetry\\dump-topology-summary.txt");
        String[] strarr = data2str(strbuff);
    }

    //1、读取文件内容，转化成StringBuffer
    public static StringBuffer getDataStr(String path){

        StringBuffer buffer = new StringBuffer();
        try {
            BufferedReader in = new BufferedReader(new FileReader(path));
            String str;
            while ((str = in.readLine()) != null) {
                //System.out.println(str);
                buffer.append(str + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //System.out.println(buffer);
        return buffer;
    }

    //2、将数据分割，每一次的抓取的结果为一条数据
    public static String[] data2str(StringBuffer strbuff){
        String data = strbuff.toString();
        //System.out.println(data);
        String[]  result = data.split("------- ");
        return result;
    }

}
