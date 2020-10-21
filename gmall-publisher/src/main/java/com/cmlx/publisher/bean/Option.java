package com.cmlx.publisher.bean;

/**
 * @Author: CMLX
 * @Description:
 * @Date: create in 2020/10/21 18:05
 */
public class Option {

    String name;
    Double value;

    public Option() {
    }

    public Option(String name, Double value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }
}
