package wikiedits;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Created by Languomao on 2019/7/29.
 */
public class WikipediaAnalysis {
    public static void main(String[] args) throws Exception {

        //可用于设置执行参数并创建从外部系统读取的源
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建一个从Wikipedia IRC日志中读取的源
        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
                .keyBy(new KeySelector<WikipediaEditEvent, String>() {
                    @Override
                    public String getKey(WikipediaEditEvent event) {
                        return event.getUser();
                    }
                });

        DataStream<Tuple2<String, Long>> result = keyedEdits
                .timeWindow(Time.seconds(5))
                .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {
                acc.f0 = event.getUser();
                acc.f1 += event.getByteDiff();
                return acc;
            }
        });

        result.print();
        see.execute();
    }
}
