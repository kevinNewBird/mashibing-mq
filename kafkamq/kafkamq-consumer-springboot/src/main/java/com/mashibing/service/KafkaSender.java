package com.mashibing.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * description: com.mashibing.service
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/11/22
 * version: 1.0
 */
@Component
public class KafkaSender {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    public void messageSender(String tpoic,String key,String message){
        try {
            System.out.println("准备发送..."+tpoic+","+","+key);
            kafkaTemplate.send(tpoic,key,message);
            System.out.println("已发送");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
