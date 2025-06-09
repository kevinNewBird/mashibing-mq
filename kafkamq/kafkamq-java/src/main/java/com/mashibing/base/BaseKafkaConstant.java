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

    public static final String BOOT_SERVERS_ACL= "192.168.231.150:9092";

    public static final String BOOT_SERVERS_IP_AUTH = "192.168.231.150:9092";

    static {
        String os = System.getProperty("os.name");
        if (StringUtils.containsIgnoreCase(os, "windows")) {
            BOOT_SERVERS = "172.22.124.60:9092,172.22.124.60:9094,172.22.124.60:9096";
        } else { // mac
            BOOT_SERVERS = "10.211.55.19:9092,10.211.55.20:9092,10.211.55.21:9092";
        }
    }
}
