package dataproduce;

/**
 * Created by Languomao on 2019/5/30.
 */
public class Student {
    private String id;
    private int create_at;
    private int update_at;
    private String name;
    private String qq;
    private String major;
    private String entryTime;
    private String school;
    private int jnshuId;
    private String dailyUrl;
    private String desire;
    private String jnshuBro;
    private String knowFrom;

    public Student(String name, String major, int jnshuId) {

    }

    public String  getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getCreate_at() {
        return create_at;
    }

    public void setCreate_at(int create_at) {
        this.create_at = create_at;
    }

    public int getUpdate_at() {
        return update_at;
    }

    public void setUpdate_at(int update_at) {
        this.update_at = update_at;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getQq() {
        return qq;
    }

    public void setQq(String qq) {
        this.qq = qq;
    }

    public String getMajor() {
        return major;
    }

    public void setMajor(String major) {
        this.major = major;
    }

    public String getEntryTime() {
        return entryTime;
    }

    public void setEntryTime(String entryTime) {
        this.entryTime = entryTime;
    }

    public String getSchool() {
        return school;
    }

    public void setSchool(String school) {
        this.school = school;
    }

    public int getJnshuId() {
        return jnshuId;
    }

    public void setJnshuId(int jnshuId) {
        this.jnshuId = jnshuId;
    }

    public String getDailyUrl() {
        return dailyUrl;
    }

    public void setDailyUrl(String dailyUrl) {
        this.dailyUrl = dailyUrl;
    }

    public String getDesire() {
        return desire;
    }

    public void setDesire(String desire) {
        this.desire = desire;
    }

    public String getJnshuBro() {
        return jnshuBro;
    }

    public void setJnshuBro(String jnshuBro) {
        this.jnshuBro = jnshuBro;
    }

    public String getKnowFrom() {
        return knowFrom;
    }

    public void setKnowFrom(String knowFrom) {
        this.knowFrom = knowFrom;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", create_at=" + create_at +
                ", update_at=" + update_at +
                ", qq='" + qq + '\'' +
                ", entryTime='" + entryTime + '\'' +
                ", school='" + school + '\'' +
                ", dailyUrl='" + dailyUrl + '\'' +
                ", desire='" + desire + '\'' +
                ", jnshuBro='" + jnshuBro + '\'' +
                ", knowFrom='" + knowFrom + '\'' +
                '}';
    }
}
