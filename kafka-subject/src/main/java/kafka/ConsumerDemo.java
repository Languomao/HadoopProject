package kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * @Classname ConsumerTest
 * @Description TODO
 * @Date 2019/11/18 17:59
 * @Created by LanKorment
 */
public class ConsumerDemo {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "1111");
        props.put("auto.offset.reset", "smallest");
        props.put("zk.connectiontimeout.ms", "15000");

        ConsumerConfig config = new ConsumerConfig(props);
        ConsumerConnector consumer =Consumer.createJavaConsumerConnector(config);
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(Config.TOPIC, Config.THREADS);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(Config.TOPIC);

        for(final KafkaStream<byte[], byte[]> kafkaStream : streams){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for(MessageAndMetadata<byte[], byte[]> mm : kafkaStream){
                        String msg = new String(mm.message());
                        System.out.println(msg);
                    }
                }

            }).start();

        }
    }
}
