package kafka.example.test1;


import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Date;

/**
 * 这个是为了将聚合结果输出
 */
public class WindowResult implements WindowFunction<Integer, Tuple3<String, Date, Integer>, Tuple, TimeWindow> {

    @Override
    public void apply(
            Tuple key,
            TimeWindow window,
            Iterable<Integer> input,
            org.apache.flink.util.Collector<Tuple3<String, Date, Integer>> out) throws Exception {
        String k = ((Tuple1<String>) key).f0;
        long windowStart = window.getStart();
        int result = input.iterator().next();
        out.collect(Tuple3.of(k, new Date(windowStart), result));

    }
}

