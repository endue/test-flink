package chapter05.domain;

/**
 * @Author:
 * @Description:
 * @Date: 2022/9/15 22:36
 * @Version: 1.0
 */
public class EventUserInfo {

    private int id;
    private String eventId;
    private int cnt;
    private String gender;
    private String city;

    public EventUserInfo(int id, String eventId, int cnt, String gender, String city) {
        this.id = id;
        this.eventId = eventId;
        this.cnt = cnt;
        this.gender = gender;
        this.city = city;
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

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
