package kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;



/**
 * 生产者实例
 * @Classname Consumer
 * @Description TODO
 * @Date 2019/11/18 17:59
 * @Created by LanKorment
 */
public class ProducerDemo {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("zk.connect", "192.168.209.121:2181");
        props.put("metadata.broker.list","192.168.209.121:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("zk.connectiontimeout.ms", "15000");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        // 发送业务消息
        // 读取文件 读取内存数据库 读socket端口
        for (int i = 1; i <= 100; i++) {
            Thread.sleep(500);
            producer.send(new KeyedMessage<String, String>(Config.TOPIC,
                    "this number ===>>> " + i));
        }

    }
}
