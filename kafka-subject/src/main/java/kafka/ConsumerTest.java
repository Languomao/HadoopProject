package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.*;

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
        props.put("zookeeper.connect", "172.18.101.21:2181,172.18.101.22:2181,172.18.101.13:2181");

        //zk连接超时
        props.put("zookeeper.session.timeout.ms", "60000");
        props.put("zookeeper.sync.time.ms", "20000");
        props.put("auto.commit.interval.ms", "10000");
        props.put("auto.offset.reset", "smallest");
        //序列化类
        props.put("serializer.class", "kafka.serializer.StringDecoder");

        //ConsumerConfig config = new ConsumerConfig(props);
        KafkaConsumer consumer =new KafkaConsumer<String , String>(props);
        consumer.subscribe(Arrays.asList("testout"));

        /*Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
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
        }*/

        long beginTime=System.currentTimeMillis();
        try {
            while (true) {
                //循环调用kafkaConsumer.poll方法获取封装在ConsumerRecord的topic消息。
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                //获取到封装在ConsumerRecords消息以后，处理获取到ConsumerRecord对象。
                for (ConsumerRecord<String, String> record : records) {
                    //简单的打印输出
                    System.out.println(
                            "offset = " + record.offset()
                                    + ",key = " + record.key()
                                    + ",value =" + record.value());
                }
            }
        } catch (Exception e) {
            //关闭kafkaConsumer
            System.out.println("消息消费结束......");
            consumer.close();
        }
        long endTime=System.currentTimeMillis();
        System.out.println("calculated using time："+(endTime-beginTime)/1000+"s");
        System.out.println("press `ctr-c` to break");


    }

}
