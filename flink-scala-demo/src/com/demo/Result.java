package com.demo;

public class Result {

    private String name;
    private Long c;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getC() {
        return c;
    }

    public void setC(Long c) {
        this.c = c;
    }

    @Override
    public String toString() {
        return "Result{" +
                "name='" + name + '\'' +
                ", c=" + c +
                '}';
    }
}
