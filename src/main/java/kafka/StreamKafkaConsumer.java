package kafka;

import kafka.model.TrafficMessage;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Properties;

public class StreamKafkaConsumer {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // 要设置启动检查点
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.10.49:9093");
        props.setProperty("zookeeper.connect", "192.168.10.49:2181");
        props.setProperty("group.id", "flink-group-01");

        //数据源配置，是一个kafka消息的消费者
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>("metric", new SimpleStringSchema(), props);

        //增加时间水位设置类
        consumer.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<String>() {
            @Override
            public long extractTimestamp(String element, long previousElementTimestamp) {
                return JSONHelper.getTimeLongFromRawMessageTraffic(element);
            }

            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
                if (lastElement != null) {
                    return new Watermark(JSONHelper.getTimeLongFromRawMessageTraffic(lastElement));
                }
                return null;
            }
        });
//        consumer.setStartFromEarliest();//从最早的开始消费
        consumer.setStartFromLatest();//从最晚的开始消费
//        consumer.setStartFromTimestamp(1645691447749L);//从特定的时间戳开始消费
//        consumer.setStartFromGroupOffsets();//默认行为，从消费者组（消费者属性中的设置）开始读取group.idKafka 代理
        // （或 Kafka 0.8 的 Zookeeper）中提交的偏移量的分区。如果找不到分区的偏移量，auto.offset.reset将使用属性中的设置。
//        还可以指定消费者应该从每个分区开始的确切偏移量（目前不需要）：
//        Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
//        specificStartOffsets.put(new KafkaTopicPartition("myTopic", 0), 23L);
//        specificStartOffsets.put(new KafkaTopicPartition("myTopic", 1), 31L);
//        specificStartOffsets.put(new KafkaTopicPartition("myTopic", 2), 43L);

//        myConsumer.setStartFromSpecificOffsets(specificStartOffsets);
        env.addSource(consumer)
                .flatMap(new Splitter())
                //以用户名为key
//                .keyBy(0,1)
                .keyBy(0).keyBy(1)
//                .keyBy("business","isp")
//                .keyBy(String.valueOf(new KeySelector<TrafficMessage, Tuple2<String, Long>>() {
//                    @Override
//                    public Tuple2<String, Long> getKey(TrafficMessage trafficMessage) throws Exception {
//                        return Tuple2.of(trafficMessage.getLabels().getBusiness(), trafficMessage.getValue());
//                    }
//                }))
                //时间窗口为10秒
//                .timeWindow(Time.seconds(10))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))//滑动时间窗口，自然时间的十秒一个点
                //将每个用户的流量总数累加起来
                .sum(2)
//                .apply()
                //输出方式是STDOUT
//                .returns(Types.TUPLE(Types.INT,Types.INT))
//                .print();
                .addSink(generateRedisSink());

        env.execute("Flink-Kafka demo");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple3<String, String, Long>> {
        @Override
        public void flatMap(String s, Collector<Tuple3<String, String, Long>> collector) throws Exception {
            TrafficMessage trafficMessage = JSONHelper.parseTraffic(s);

            if (null != trafficMessage) {
//                collector.collect(new Tuple2<>(trafficMessage.getLabels().getSn(), trafficMessage.getValue()));
                collector.collect(new Tuple3<String, String, Long>(trafficMessage.getLabels().getIsp(), trafficMessage.getLabels().getBusiness(), trafficMessage.getValue()));
//                collector.collect(new Tuple2<>(trafficMessage.getLabels().getBusiness(), trafficMessage.getValue()));
//                collector.collect(new Tuple2<>(trafficMessage.getLabels().getUid(), trafficMessage.getValue()));
            }
        }
    }

    public class WholeLoad extends RichMapFunction<String, TrafficMessage.LabelsDTO> {
        @Override
        public TrafficMessage.LabelsDTO map(String s) throws Exception {
            return null;
        }
    }

    /**
     * 生成RedisSink
     */
    private static RedisSink<Tuple3<String, String, Long>> generateRedisSink() {
        // Redis配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setMaxTotal(8) // 最大实例总数
                .setMaxIdle(4) // 实例最多空闲数
                .setMinIdle(2)
                .setDatabase(1)
                .setHost("192.168.10.33")
                .setPort(6381)
                .setPassword("sfahh38yrh32r289")
                .build();
        // Mapper
        RedisMapper<Tuple3<String, String, Long>> redisMapper = new RedisMapper<Tuple3<String, String, Long>>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
//                // 定义保存数据到Redis的命令
                return new RedisCommandDescription(
                        RedisCommand.SET // 使用hset命令
//                        tuple2.f0+tuple2.f1
//                        "my_hash" // 表名
                );
            }

            @Override
            public String getKeyFromData(Tuple3<String, String, Long> tuple2) {
                StringBuffer buffer = new StringBuffer();
                buffer.append(tuple2.f0);
                buffer.append("-");
                buffer.append(tuple2.f1);
                buffer.append(":");
                buffer.append(System.currentTimeMillis());
                return buffer.toString();
            }

            @Override
            public String getValueFromData(Tuple3<String, String, Long> tuple2) {
                return tuple2.f2.toString();
            }
        };

        return new RedisSink<>(config, redisMapper);
    }

}
