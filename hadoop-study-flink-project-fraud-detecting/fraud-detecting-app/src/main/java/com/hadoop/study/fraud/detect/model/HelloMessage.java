package com.hadoop.study.fraud.detect.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class HelloMessage {

    private String name;

    @Override
    public String toString() {
        return "name='" + name;
    }
}
