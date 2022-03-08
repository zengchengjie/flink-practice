package kafka;

import com.alibaba.fastjson.JSONObject;
import kafka.model.SingleMessage;
import kafka.model.TrafficMessage;

/**
 * @Description: 解析原始消息的辅助类
 * @author: willzhao E-mail: zq2599@gmail.com
 * @date: 2019/1/1 20:13
 */
public class JSONHelper {

    /**
     * 解析消息，得到时间字段
     * @param raw
     * @return
     */
    public static long getTimeLongFromRawMessage(String raw){
        SingleMessage singleMessage = parse(raw);
        return null==singleMessage ? 0L : singleMessage.getTimeLong();
    }
    /**
     * 解析消息，得到时间字段(毫秒数)
     * @param raw
     * @return
     */
    public static long getTimeLongFromRawMessageTraffic(String raw){
        TrafficMessage trafficMessage = parseTraffic(raw);
        return null==trafficMessage ? 0L : trafficMessage.getTimestamp().getTime();
    }

    /**
     * 将消息解析成对象
     * @param raw
     * @return
     */
    public static SingleMessage parse(String raw){
        SingleMessage singleMessage = null;

        if (raw != null) {
            singleMessage = JSONObject.parseObject(raw, SingleMessage.class);
        }

        return singleMessage;
    }
    /**
     * 将消息解析成对象
     * @param raw
     * @return
     */
    public static TrafficMessage parseTraffic(String raw){
        TrafficMessage trafficMessage = null;

        if (raw != null) {
            trafficMessage = JSONObject.parseObject(raw, TrafficMessage.class);
        }

        return trafficMessage;
    }
}
