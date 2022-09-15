package chapter05.domain;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:
 * @Description:
 * @Date: 2022/9/15 22:24
 * @Version: 1.0
 */
public class EventCount {
    private int id;
    private String eventId;
    private int cnt;

    public EventCount(int id, String eventId, int cnt) {
        this.id = id;
        this.eventId = eventId;
        this.cnt = cnt;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public int getCnt() {
        return cnt;
    }

    public void setCnt(int cnt) {
        this.cnt = cnt;
    }
}
