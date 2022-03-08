package kafka.model;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

import java.util.Date;

@Data
public class TrafficMessage {
    public TrafficMessage() {
    }

    private Metric.LabelsDTO labels;
    private String name;
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date timestamp;
    private Long value;

    @Data
    public static class LabelsDTO {
        public LabelsDTO() {
        }

        private String name;
        private String business;
        private String city;
        private String device;
        private String hostname;
        private String instance;
        private String isp;
        private String job;
        private String port;
        private String sn;
        private String status;
        private String uid;
    }
}
