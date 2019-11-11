package udf3;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;
/**
 * Created by Languomao on 2018/7/10.
 *
 *
 *
 */

public class GetCommentNameOrId extends UDF {
    public String evaluate(String url,String flag){
        String str = null;
        Pattern p = Pattern.compile(flag+"/[a-zA-Z0-9]+");
        Matcher m = p.matcher(url);
        if(m.find()){
            str = m.group(0).toLowerCase().split("/")[1];
        }
        return str;
    }

    public static void main(String[] args) {
        String url = "http://cms.yhd.com/sale/vtxqCLCzfto?tc=ad.0.0.17280-32881642.1&tp=1.1.36.9.1.LEffwdz-10-35RcM&ti=ZX8H";
        GetCommentNameOrId gs = new GetCommentNameOrId();
        System.out.println(gs.evaluate(url,"sale"));
    }
}