package com.mashibing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * description: com.mashibing
 * company: 北京海量数据有限公司
 * create by: zhaosong 2023/3/29
 * version: 1.0
 */
public class LogTest {

    private static Logger logger = LoggerFactory.getLogger(LogTest.class);

    public static void main(String[] args) {
        logger.info("this is info content!!!");
        logger.debug("this is debug content!!!");
        logger.error("this is error content!!!");
    }
}
