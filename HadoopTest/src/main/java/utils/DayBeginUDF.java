package utils;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Languomao on 2018/7/10.
 */


@Description(name = "udf_getdaybegin",
        value = "getdaybegin",
        extended = "getdaybegin() ;\r\n"
                + " getdaybegin(2) \r\n"
                + " getdaybegin('2017/06/29 01:02:03') \r\n"
                + " getdaybegin('2017/06/29 01:02:03',2) \r\n"
                + " getdaybegin(date_obj) \r\n"
                + " getdaybegin(date_obj,2)")
public class DayBeginUDF extends UDF{

    /**
     * 计算现在的起始时刻(毫秒数)
     */
    public long  evaluate(){
        return DateUtil.getDayBeginTime(new Date()).getTime();
    }

    public long  evaluate(Date d){

        return DateUtil.getDayBeginTime(d).getTime();
    }

    public long  evaluate(int offset){
        return DateUtil.getDayBeginTime(new Date(),offset).getTime();
    }

    public long  evaluate(Date d,int offset){
        return DateUtil.getDayBeginTime(d,offset).getTime();
    }

    public  long  evaluate(String dateStr) throws Exception{
        SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date d = df.parse(dateStr);
        return evaluate(d);
    }
    public long  evaluate(String dateStr,int offset) throws Exception{
        SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date d = df.parse(dateStr);
        return evaluate(d,offset);
    }
    public long  evaluate(String dateStr,String fmt) throws Exception{
        SimpleDateFormat df = new SimpleDateFormat(fmt);
        Date d = df.parse(dateStr);
        return evaluate(d);
    }
    public long  evaluate(String dateStr,int offset, String fmt) throws Exception{
        SimpleDateFormat df = new SimpleDateFormat(fmt);
        Date d = df.parse(dateStr);
        return evaluate(d,offset);
    }


}