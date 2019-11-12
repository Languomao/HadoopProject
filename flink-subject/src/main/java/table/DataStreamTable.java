package table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * Created by Languomao on 2019/7/30.
 */
public class DataStreamTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "啤酒", 21L),
                new Order(1L, "雪碧", 2L),
                new Order(2L, "可乐", 8L)
        ));

        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(3L, "王老吉", 2L),
                new Order(4L, "红牛", 3L),
                new Order(4L, "哇哈哈", 2L)
        ));

        Table tableA = tEnv.fromDataStream(orderA, "userId,product,amount");

        //注册表
        tEnv.registerDataStream("orderB", orderB, "userId,product,amount");

        //查询表
        Table result = tEnv.sqlQuery("select * from " + tableA + " where userId=1 union all select * from orderB " +
                "where amount > 2");

        DataStream<Order> resultStream = tEnv.toAppendStream(result, Order.class);

        resultStream.print();

        env.execute("table");
    }

    public static class Order {
        public Long userId;
        public String product;
        public Long amount;

        public Order() {
        }

        public Order(Long userId, String product, Long amount) {
            this.userId = userId;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{" + "userId=" + userId + ", product='" + product + '\'' + ", amount=" + amount + '}';
        }
    }
}
