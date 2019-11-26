package hive;

import java.sql.*;
/**
*
* 连接hive数据库,做DDL等操作
* 主要接口为execute和executeQuery
*
* */
public class HiveDemo {

        //    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
        //jdbc核心驱动类
        private static String driverName = "org.apache.hive.jdbc.HiveDriver";

        public static void main(String[] args) throws SQLException {
            try {
                //加载jdbc核心驱动类
                Class.forName(driverName);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }
//        Connection conn = DriverManager.getConnection("jdbc:hive://192.168.145.130:10000/test","root","root");
            //建立连接
            Connection conn = DriverManager.getConnection("jdbc:hive2://10.1.1.7:10000/test","root","hadoop123");
            //创建执行器
            Statement sta = conn.createStatement();

            System.out.println("connect success!");

            String tableName = "test1";
            //定义执行sql语句
            sta.execute("use test");
            //sta.execute("drop table if exists " + tableName);
            //sta.execute("create table " + tableName + " (id int,name string)");
            String sql = "select * from test1 limit 10";
            ResultSet res1 = sta.executeQuery(sql);
            while(res1.next()){
                System.out.println(res1.getString(1)+"\t"+ res1.getString(2)+"\t"+ res1.getString(3));
            }
            System.out.println("success!");

            //定义执行结果集和sql语句
            /*ResultSet res2 = sta.executeQuery("desc " + tableName );
            while(res2.next()){
                System.out.println(res2.getString(1));
            }*/
        }
}
