package kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import structured_streaming.utils.FileUtil;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 将制定目录下的文件生产为消息
 *
 * @Classname ProducerTest
 * @Description TODO
 * @Date 2019/11/19 11:29
 * @Created by LanKorment
 */
public class ProducerTest {

    public static void main(String[] args){
        Properties props = new Properties();
        props.put("zookeeper.connect", "172.18.101.21:2181,172.18.101.22:2181,172.18.101.13:2181");
        props.put("metadata.broker.list","172.18.101.22:9092,172.18.101.13:9092,172.18.101.14:9092");
        props.put("group.id", "1111");
        props.put("auto.offset.reset", "smallest");
        props.put("zk.connectiontimeout.ms", "60000");
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        //ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new KafkaProducer<>(props);

        String filePath = "D:\\WorkSpace\\Telemetry\\";
        List<String> messages = FileUtil.readFileToLine(filePath);
        if(messages!=null && messages.size()>0){
            System.out.println("total records："+messages);
            long beginTime=System.currentTimeMillis();
            for(int i=0;i<messages.size();i++){
                producer.send(new ProducerRecord<>(Config.TOPIC,messages.get(i)));
                //System.out.println(messages.get(i));
            }
            long endTime=System.currentTimeMillis();
            System.out.println("calculated using time："+(endTime-beginTime)+"ms");
            producer.close();
        }else{
            System.out.println("消息为空");
            System.exit(-2);
    }

    }


}
