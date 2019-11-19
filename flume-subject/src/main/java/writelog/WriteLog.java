package writelog;

import java.util.Date;
import org.apache.log4j.Logger;

/**
 * Created by Languomao on 2018/7/12.
 */
public class WriteLog {
    protected static final Logger logger = Logger.getLogger(WriteLog.class);

    public static void main(String[] args) throws Exception {
        while (true) {
            logger.info("Hello,languomao:"+ String.valueOf(new Date().getTime()));
            Thread.sleep(5000);
        }
    }
}
