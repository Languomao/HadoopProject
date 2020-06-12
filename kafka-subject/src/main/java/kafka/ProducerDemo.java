package kafka;

/**
 * 生产者实例
 * @Classname ConsumerTest
 * @Description TODO
 * @Date 2019/11/18 17:59
 * @Created by LanKorment
 */
public class ProducerDemo {
    /*public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        //zookeeper队列
        props.put("zookeeper.connect", "localhost:2181");
        //kafka队列
        props.put("metadata.broker.list","localhost:9092");
        //序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //props.put("advertised.host.name", "10.1.1.3");
        props.put("zk.connectiontimeout.ms", "30000");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        // 发送业务消息
        // 读取文件 读取内存数据库 读socket端口
        for (int i = 1; i <= 100; i++) {
            //System.out.println("开始生产消息····");
            Thread.sleep(50);
            producer.send(new KeyedMessage(Config.TOPIC, "this number ===>>> " + i));
            //System.out.println("成功生产消息····");
        }

    }*/
}
