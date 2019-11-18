package udf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by Languomao on 2019/11/18.
 */
public class HelloUDF extends UDF {
    public Text evaluate(Text input){
        return new Text("Hello : "+input);
    }

    public static void main(String[] args){

        HelloUDF udf = new HelloUDF();
        Text result = udf.evaluate(new Text("languomao"));

        System.out.println(result.toString());
    }
}