
package com.mashibing.streams.timer;

import com.mashibing.base.BaseKafkaConstant;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 类说明：消费者入门
 */
public class StreamTimeConsumer {

    public static final String WINDOW_TOPIC = "output-window";

    public static void main(String[] args) {
        // 设置属性
        Properties properties = new Properties();
        // 指定连接的kafka服务器的地址
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BaseKafkaConstant.BOOT_SERVERS);
        // 设置String的反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group001");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  //最早：从头开始消费
        // 构建kafka消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        try {
            consumer.subscribe(Collections.singletonList(WINDOW_TOPIC));
            // 调用消费者拉取消息
            while (true) {
                // 设置1秒的超时时间
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    System.out.println("接收到消息: key = " + key + ", value = " + value);
                }
            }
        } finally {
            // 释放连接
            consumer.close();

        }

    }


}