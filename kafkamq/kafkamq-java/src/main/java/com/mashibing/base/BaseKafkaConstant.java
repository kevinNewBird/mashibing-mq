package com.mashibing.base;


import org.apache.commons.lang3.StringUtils;

/**
 * description: com.mashibing.base
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/11/8
 * version: 1.0
 */
public abstract class BaseKafkaConstant {

    public static final String BOOT_SERVERS;

    static {
        String os = System.getProperty("os.name");
        if (StringUtils.containsIgnoreCase(os, "windows")) {
            BOOT_SERVERS = "172.22.124.60:9092,172.22.124.60:9094,172.22.124.60:9096";
        } else { // mac
            BOOT_SERVERS = "10.211.55.13:9094,10.211.55.13:9096,10.211.55.13:9098";
        }
    }
}
