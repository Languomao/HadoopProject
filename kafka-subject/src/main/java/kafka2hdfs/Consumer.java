package kafka2hdfs;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Classname Consumer
 * Description TODO
 * Date 2020/6/9 17:16
 * Created by LanKorment
 * @Message hadoop-hdfs&hadoop-common&hadoop-mapreduce-client-core:3.0.0 , kafka_2_12&kafka-clients:2.2.1
 */
public class Consumer {
    private static KafkaConsumer<String, String> consumer;    //接受数据方法
    private static Properties props;           //传递的参数

    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
    static String now = sdf.format(new Date());

    static {
        props = new Properties();
        //消费者kafka地址 不用和producer地址一样，必须指定属性
        props.put("bootstrap.servers", "172.18.101.14:9092,172.18.101.13:9092,172.18.101.22:9092");
        props.put("zookeeper.connect", "172.18.101.21:2181,172.18.101.13:2181,172.18.101.22:2181");
        //允许自动提交位移
        props.put("enable.auto.commit", true);
        // 从最早的消息开始读取
        props.put("auto.offset.reset", "earliest");

        //key序列化与反序列化，必须指定
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //组，必须指定属性
        props.put("group.id", "test1");
    }

    private static void ConsumerMessage() {
        //HDFSWriter hdfsWriter = new HDFSWriter();

        // 使用创建的Properties实例构造consumer实例
        consumer = new KafkaConsumer<String, String>(props);
        //订阅topic。调用kafkaConsumer.subscribe方法订阅consumer group所需的topic列表
        consumer.subscribe(Arrays.asList("test"));


        //使用轮询拉取数据--消费完成之后会根据设置时长来清除消息，被消费过的消息，如果想再次被消费，可以根据偏移量(offset)来获取
        try {
            //保存数据的文件夹，如果没有创建
            String path = "/dada/telemetry/srcdata";
            File file = new File(path);
            //file.mkdir();

            //将kafka的数据取出来，存放在其他位置或者文件中
            while (true) {
                //循环调用kafkaConsumer.poll方法获取封装在ConsumerRecord的topic消息。
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));   //获取数据
                FileWriter fw = new FileWriter(file + "/dump-" + now, true);
                //获取到封装在ConsumerRecords消息以后，处理获取到ConsumerRecord对象。
                for (ConsumerRecord<String, String> r : records) {
                    //打印输出
                    System.out.printf("topic = %s, offset = %s, key = %s, value = %s", r.topic(), r.offset(),
                            r.key(), r.value());
//                    hdfsWriter.writer(r.toString());
                    //将消费消息写入新的文件中
                    fw.write(r.value() + "\n");
                    fw.flush();
                }
                fw.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
            consumer.close();
            System.out.println("consumer interrupt....");
        } finally {
            consumer.close();
            System.out.println("close consumer...");
        }
    }

    public static void main(String[] args) {
        Timer timer = new Timer();
        //安排指定的任务从指定的延迟后开始进行重复的固定延迟执行 0：即时执行
        timer.schedule(new Collection(),0,60*60*1000);
        timer.schedule(new ClearTask(),0,60*60*1000);
        ConsumerMessage();

    }
}
