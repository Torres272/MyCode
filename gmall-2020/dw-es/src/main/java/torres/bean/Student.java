package torres.bean;

public class Student {
    private int stuId;
    private String name;

    public Student(int stuId, String name) {
        this.stuId = stuId;
        this.name = name;
    }

    public Student() {
    }

    public int getStuId() {
        return stuId;
    }

    public String getName() {
        return name;
    }

    public void setStuId(int stuId) {
        this.stuId = stuId;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Student{" +
                "stuId=" + stuId +
                ", name='" + name + '\'' +
                '}';
    }
}
