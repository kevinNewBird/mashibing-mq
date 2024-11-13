package com.mashibing.interceptor;

import com.mashibing.base.BaseKafkaConstant;
import com.mashibing.base.BaseTest;
import com.mashibing.pojo.User;
import com.mashibing.serializer.impl.UserDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * description: com.mashibing.serializer
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/11/12
 * version: 1.0
 */
public class ConsumerInterceptor extends BaseTest {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerInterceptor.class);

    private static final String GROUP_ID = "group01";

    /**
     * description: 创建主题
     * create by: zhaosong 2024/11/12 11:20
     */
    @Before
    public void createTopic() {
        createTopic(ProducerInterceptor.USER_TOPIC, 3, (short) 2);
    }

    /**
     * description: 消费消息
     * create by: zhaosong 2024/11/12 11:21
     */
    @Test
    public void consume() {
        // 构建消费者连接/配置信息
        Properties props = initBaseConf();

        // 构建生产者对象
        try (KafkaConsumer<String, User> consumer = new KafkaConsumer<>(props);) {
            // 订阅消费TOPIC
            consumer.subscribe(Collections.singleton(ProducerInterceptor.USER_TOPIC));
            // 循环拉取信息（监听）
            while (true) {
                // 每隔2秒，拉取准备就绪的信息列表
                ConsumerRecords<String, User> recordList = consumer.poll(Duration.ofSeconds(2));
                if (recordList.isEmpty()) {
                    continue;
                }

                // 模拟消息消费
                for (ConsumerRecord<String, User> record : recordList) {
                    int partition = record.partition();
                    long offset = record.offset();
                    logger.info("topic: {},key: {}, value: {}, partition: {}, offset: {}"
                            , record.topic(), record.key(), record.value(), partition, offset);
                }
            }
        } catch (Exception ex) {
            logger.error("Failed to receive message!", ex);
        }
    }

    /**
     * 初始化消费者连接/配置信息
     * description:
     * create by: zhaosong 2024/11/12 11:22
     *
     * @return
     */
    private Properties initBaseConf() {
        Properties props = new Properties();
        // 连接信息
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BaseKafkaConstant.BOOT_SERVERS);
        // 序列化
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDeserializer.class.getName());
        // 指定消费分组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        return props;
    }
}
