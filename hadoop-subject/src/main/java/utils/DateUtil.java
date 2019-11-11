package utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by Languomao on 2018/7/10.
 */

public class DateUtil {
    /**
     * 得到指定date的零时刻.
     */
    public static Date getDayBeginTime(Date d) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd 00:00:00");
            return sdf.parse(sdf.format(d));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 得到指定date的偏移量零时刻.
     */

    public static Date getDayBeginTime(Date d,int offset) {
        try {
            Date date=getDayBeginTime(d);
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            c.add(Calendar.DAY_OF_MONTH,offset);
            return c.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 得到指定date所在周的起始时刻.
     */
    public static Date getWeekBeginTime(Date d) {
        try {
            Date date=getDayBeginTime(d);
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            int i = c.get(Calendar.DAY_OF_WEEK);
            c.add(Calendar.DAY_OF_MONTH,-(i-1));
            return c.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    /**
     * 得到指定date所在周的结束时刻.
     */
    public static Date getWeekEndTime(Date d) {
        try {
            Date date=getDayBeginTime(d);
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            int i = c.get(Calendar.DAY_OF_WEEK);
            c.add(Calendar.DAY_OF_MONTH,(8-i));
            return c.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    /**
     * 得到指定date所在周的结束时刻.
     */
    public static Date getWeekEndTime(Date d,int offset) {
        try {
            Date date=getDayBeginTime(d);
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            int i = c.get(Calendar.DAY_OF_WEEK);
            c.add(Calendar.DAY_OF_MONTH,(8-i));
            c.add(Calendar.DAY_OF_MONTH,offset*7);
            return c.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 得到指定date所在周的起始时刻.
     */
    public static Date getWeekBeginTime(Date d,int offset) {
        try {
            Date date=getDayBeginTime(d);
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            int i = c.get(Calendar.DAY_OF_WEEK);
            c.add(Calendar.DAY_OF_MONTH,-(i-1));
            c.add(Calendar.DAY_OF_MONTH,offset*7);
            return c.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    /**
     * 得到指定date所在月的起始时刻.
     */
    public static Date getMonthBeginTime(Date d ) {
        try {
            Date date=getDayBeginTime(d);
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            int i = c.get(Calendar.DAY_OF_MONTH);
            c.add(Calendar.DAY_OF_MONTH,-(i-1));
            return c.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    /**
     * 得到指定date所在月的起始时刻.
     */
    public static Date getMonthBeginTime(Date d,int offset) {
        try {
            Date date=getDayBeginTime(d);
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            int i = c.get(Calendar.DAY_OF_MONTH);
            c.add(Calendar.DAY_OF_MONTH,-(i-1));
            c.add(Calendar.MONTH,offset);
            return c.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
