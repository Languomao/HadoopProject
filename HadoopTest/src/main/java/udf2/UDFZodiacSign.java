package udf2;


import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by Languomao on 2018/7/10.
 */
public class UDFZodiacSign extends UDF {

    private SimpleDateFormat df;

    public UDFZodiacSign() {
        df = new SimpleDateFormat("MM-dd-yyyy");
    }

    public String evaluate(Date bday) {
        return this.evaluate(bday.getMonth(), bday.getDay());
    }

    public String evaluate(String bday) {
        Date date = null;
        try {
            date = df.parse(bday);
        } catch (Exception ex) {
            return null;
        }

        return this.evaluate(date.getMonth() + 1, date.getDay());
    }

    public String evaluate(Integer month, Integer day) {
        if (month == 1) {
            if (day < 20) {
                return "Capricorn";
            } else {
                return "Aquarius";
            }
        }
        if (month == 2) {
            if (day < 19) {
                return "Aquarius";
            } else {
                return "Pisces";
            }
        }
		/* ...other months here */
        return null;

    }
}
