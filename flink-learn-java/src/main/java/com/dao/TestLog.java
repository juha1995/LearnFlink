package com.dao;

public class TestLog {

    String date;
    String url;
    String name;

    //计数用的字段
    int cnt;

    public TestLog(String date, String url, String name , int cnt) {
        this.date = date;
        this.url = url;
        this.name = name;
        this.cnt = cnt;
    }

    @Override
    public String toString() {
        return "TestLog{" +
                "date='" + date + '\'' +
                ", url='" + url + '\'' +
                ", name='" + name + '\'' +
                ", cnt=" + cnt +
                '}';
    }

    public String getDate() {
        return date;
    }

    public int getCnt() {
        return cnt;
    }

    public void setCnt(int cnt) {
        this.cnt = cnt;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public TestLog() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
