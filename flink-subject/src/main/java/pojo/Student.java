package pojo;

/**
 * Created by Languomao on 2019/7/30.
 */
public class Student {
    public String id;
    public String name;
    public String course;
    public String score;

    public Student(){
        super();
    }
    public Student(String id, String name, String course, String score) {
        this.id = id;
        this.name = name;
        this.course = course;
        this.score = score;
    }

}
