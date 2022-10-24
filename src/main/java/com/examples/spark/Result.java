package com.examples.spark;

import java.io.Serializable;
import java.util.ArrayList;

public class Result implements Serializable {
    String values;
    String delimiter;
    String replacedValues;
    ArrayList<String> list;

    Result(String values, String delimiter) {
        this.values = values;
        this.delimiter = delimiter;
        this.list = new ArrayList<>();
    }

    public String getValues() {
        return values;
    }

    public void setValues(String values) {
        this.values = values;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public ArrayList<String> getList() {
        return list;
    }

    public void setList(ArrayList<String> list) {
        this.list = list;
    }

    public String getReplacedValues() {
        return replacedValues;
    }

    public void setReplacedValues(String replacedValues) {
        this.replacedValues = replacedValues;
    }

}
