package com.cmlx.publisher.bean;

import java.util.List;

/**
 * @Author: CMLX
 * @Description:
 * @Date: create in 2020/10/21 18:04
 */
public class OptionGroup {

    String title;
    List<Option> options;

    public OptionGroup() {
    }

    public OptionGroup(String title, List<Option> options) {
        this.title = title;
        this.options = options;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Option> getOptions() {
        return options;
    }

    public void setOptions(List<Option> options) {
        this.options = options;
    }
}
