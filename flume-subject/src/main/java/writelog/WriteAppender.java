package writelog;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Created by Languomao on 2018/7/13.
 */

public class WriteAppender extends AppenderSkeleton {
    private String account ;

    @Override
    protected void append(LoggingEvent event) {
        System.out.println("Hello, " + account + " : "+ event.getMessage());
    }

    public void close() {
        // TODO Auto-generated method stub

    }

    public boolean requiresLayout() {
        // TODO Auto-generated method stub
        return false;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }
}
