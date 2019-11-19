package kafka;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.javaapi.producer.Producer;
import utils.FileUtil;

import java.util.List;
import java.util.Properties;

/**
 * 消费消息
 *
 * @Classname ProducerTest
 * @Description TODO
 * @Date 2019/11/19 11:29
 * @Created by LanKorment
 */
public class ProducerTest {

    public static void main(String[] args){
        Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:2181");
        props.put("metadata.broker.list","localhost:9092");
        props.put("group.id", "1111");
        props.put("auto.offset.reset", "smallest");
        props.put("zk.connectiontimeout.ms", "15000");
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        String filePath = "E:\\data\\video1w";
        List<String> messages = FileUtil.readFileToLine(filePath);
        if(messages!=null && messages.size()>0){
            System.out.println("total records："+messages);
            long beginTime=System.currentTimeMillis();
            for(int i=0;i<messages.size();i++){
                producer.send(new KeyedMessage<String , String>(Config.TOPIC,messages.get(i)));
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
