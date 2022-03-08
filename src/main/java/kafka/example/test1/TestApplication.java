package kafka.example.test1;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TestApplication {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String,Long>> dataStream = env.addSource(new MySource());

        dataStream.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .aggregate(new CountAggregate(), new WindowResult()
                ).print();

        env.execute();
    }
}
