package com.demo;

public class Info {

	private String name;
	private Integer c;
	private Long ts;

	public Info() {

	}

	public Info(String name) {
		this.name = name;
		this.c = 1;
		this.ts = System.currentTimeMillis();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getC() {
		return c;
	}

	public void setC(Integer c) {
		this.c = c;
	}

	public Long getTs() {
		return ts;
	}

	public void setTs(Long ts) {
		this.ts = ts;
	}

	@Override
	public String toString() {
		return "Info [name=" + name + ", c=" + c + ", ts=" + ts + "]";
	}

}
