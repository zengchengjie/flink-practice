package kafka.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 每隔1秒统计最近2秒的单词数量
 */
public class JavaFlinkStreamingDemo {

    public static void main(String[] args) throws Exception {

        //拿到Stream执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //基于socket 构建数据源 数据源有几种，后面再演示
        DataStreamSource<String> stream = env.socketTextStream("localhost", 8888);
        /**
         *  第一个泛形是输入类型
         *  第二个是返回的类型
         */
        SingleOutputStreamOperator<WordCount> flatStream = stream.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String value, Collector<WordCount> out) throws Exception {
                String[] split = value.split(",");
                for (String word : split) {
                    out.collect(new WordCount(word, 1));
                }
            }
        });
        //这里根据对象的字段进行keyBy然后统计，最后聚合
        SingleOutputStreamOperator<WordCount> sum = flatStream.keyBy("word")
                .timeWindow(Time.seconds(2), Time.seconds(1))
                .sum("count");

        sum.print();

        env.execute("javaFlinkStreamingDemo");
    }

    public static class WordCount {

        public String word;

        public long count;

        public WordCount() {

        }

        public WordCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "单词：" + word + "数量" + count;
        }
    }

}