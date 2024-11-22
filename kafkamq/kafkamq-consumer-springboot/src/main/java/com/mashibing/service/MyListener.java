package com.mashibing.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * description: com.mashibing.service
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/11/22
 * version: 1.0
 */
public class MyListener {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());


    @KafkaListener(topics = {"traffic-shaping-result"})
    public void listen(ConsumerRecord<?, ?> record) {
        logger.info("收到服务器的应答: " + record.value().toString());
    }
}
