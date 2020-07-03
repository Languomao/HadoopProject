package kafka;

import java.io.File;
import java.io.FileWriter;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Classname KafkaCunsumer
 * Date 2020/6/9 16:45
 * Created by LanKorment
 * path:https://www.cnblogs.com/zqzdong/p/6438962.html
 * hadoop-hdfs&hadoop-common&hadoop-mapreduce-client-core:3.0.0 , kafka&kafka-clients:2.2.1
 */

public class KafkaConsumerTest {
    public static void main(String[] args) {
        String topicName = "test";
        String groupId = "group1";
        //构造java.util.Properties对象
        Properties props = new Properties();
        // 必须指定属性。
        //props.put("bootstrap.servers", "172.18.101.14:9092,172.18.101.13:9092,172.18.101.22:9092");
        //props.put("zookeeper.connect", "172.18.101.21:2181,172.18.101.13:2181,172.18.101.22:2181");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        // 必须指定属性。
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        // 从最早的消息开始读取
        props.put("auto.offset.reset", "earliest");
        // 必须指定
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 必须指定
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 使用创建的Properties实例构造consumer实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        // 订阅topic。调用kafkaConsumer.subscribe方法订阅consumer group所需的topic列表
        consumer.subscribe(Arrays.asList(topicName));
        try {
            String path = "D:\\WorkSpace\\data&note";
            File file = new File(path);
            //file.mkdir();

            while (true) {
                //循环调用kafkaConsumer.poll方法获取封装在ConsumerRecord的topic消息。
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));
                //获取到封装在ConsumerRecords消息以后，处理获取到ConsumerRecord对象。
                FileWriter fw = new FileWriter(file + "/test", true);
                for (ConsumerRecord<String, String> record : records) {
                    //简单的打印输出,一行数据为一条消息
                    System.out.println(
                            "offset = " + record.offset()
                                    + ",key = " + record.key()
                                    + ",value =" + record.value());
                    fw.write(record.value()+"\n");
                    fw.flush();
                }
                fw.close();
            }
        } catch (Exception e) {
            //关闭kafkaConsumer
            System.out.println("消息消费结束......");
            consumer.close();
        }
        System.out.println("关闭消费者......");
    }
}

