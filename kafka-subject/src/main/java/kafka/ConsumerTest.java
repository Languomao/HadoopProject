package kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 将对应路径下的文件生产为消息
 *
 * @Classname ConsumerTest
 * @Description TODO
 * @Date 2019/11/19 11:29
 * @Created by LanKorment
 */
public class ConsumerTest {

    public static void main(String[] args){
        Properties props = new Properties();
        props.put("group.id", "group-1");
        props.put("zookeeper.connect", "localhost:2181");

        //zk连接超时
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        //序列化类
        props.put("serializer.class", "kafka.serializer.StringDecoder");

        ConsumerConfig config = new ConsumerConfig(props);
        ConsumerConnector consumer =Consumer.createJavaConsumerConnector(config);


        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(Config.TOPIC, new Integer(1));

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
        while(true){
            List<KafkaStream<String, String>> streams = consumerMap.get(Config.TOPIC);
            for (final KafkaStream stream : streams) {
                ConsumerIterator<String, String> it = stream.iterator();
                long beginTime=System.currentTimeMillis();
                while (it.hasNext()) {
                    System.out.println("this is kafka consumer : " +  it.next().message().toString() );
                }
                long endTime=System.currentTimeMillis();
                System.out.println("calculated using time："+(endTime-beginTime)/1000+"s");
                System.out.println("press `ctr-c` to break");
            }
        }
    }

}
