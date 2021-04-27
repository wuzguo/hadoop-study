package com.hadoop.study.mapreduce.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Objects;

/**
 * <B>说明：</B><BR>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2020/8/11 15:30
 */

@Getter
@AllArgsConstructor
public enum TypeEnum {

    PRODUCT(0, "product"),
    ORDER(1, "order");

    private Integer code;

    private String value;


    public static TypeEnum valueOf(Integer code) {
        for (TypeEnum typeEnum : TypeEnum.values()) {
            if (Objects.equals(code, typeEnum.code)) {
                return typeEnum;
            }
        }
        return null;
    }
}
