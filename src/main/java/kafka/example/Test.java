package kafka.example;

import com.alibaba.fastjson.JSONObject;
import kafka.model.TrafficMessage;

public class Test {
    public static void main(String[] args) {
        String str="{\"labels\":{\"__name__\":\"node_network_transmit_bytes_total\",\"business\":\"腾讯\",\"city\":\"福建\",\"device\":\"eth0\",\"hostname\":\"FUJIAN_CTCC_001\",\"instance\":\"192.168.10.132:9100\",\"isp\":\"电信\",\"job\":\"node\",\"port\":\"9100\",\"sn\":\"192.168.10.132\",\"status\":\"启用\",\"uid\":\"803\"},\"name\":\"node_network_transmit_bytes_total\",\"timestamp\":\"2022-03-04T10:11:32Z\",\"value\":\"312953560\"}";
        TrafficMessage trafficMessage = null;

        if (str != null) {
            trafficMessage = JSONObject.parseObject(str, TrafficMessage.class);
        }
        System.out.println(trafficMessage.getName());
        System.out.println(trafficMessage.getTimestamp());
        System.out.println(trafficMessage.getTimestamp().getTime());
    }
}
