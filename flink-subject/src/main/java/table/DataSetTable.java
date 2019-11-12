package table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * Created by Languomao on 2019/7/30.
 */
public class DataSetTable {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
        DataSet<WC> input = env.fromElements(
                new WC("hello", 1),
                new WC("word", 1),
                new WC("hello", 1)
        );
        Table table = tEnv.fromDataSet(input);
        Table filtered = table.groupBy("word").select("word, frequency.sum as frequency");
        DataSet<WC> result = tEnv.toDataSet(filtered, WC.class);
        result.print();
    }

    public static class WC {
        public String word;
        public long frequency;

        public WC() {
        }

        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", frequency=" + frequency +
                    '}';
        }
    }
}
