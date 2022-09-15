package chapter05.domain;

/**
 * @Author:
 * @Description:
 * @Date: 2022/9/15 22:23
 * @Version: 1.0
 */
public class UserInfo {

    private int id;
    private String gender;
    private String city;

    public UserInfo(int id, String gender, String city) {
        this.id = id;
        this.gender = gender;
        this.city = city;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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
