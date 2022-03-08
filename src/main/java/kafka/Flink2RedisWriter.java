package kafka;

import kafka.model.SinkResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.List;

public class Flink2RedisWriter extends RichSinkFunction<List<SinkResult>> {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void invoke(List<SinkResult> value, Context context) throws Exception {
        super.invoke(value, context);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
