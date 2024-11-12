package com.mashibing.pojo;

import lombok.*;

/**
 * description: com.mashibing.serializer
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/11/12
 * version: 1.0
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class User {
    private int id;

    private String name;

}
