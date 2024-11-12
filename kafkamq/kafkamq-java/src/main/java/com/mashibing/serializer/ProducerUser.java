package com.mashibing.serializer;

import com.mashibing.base.BaseKafkaConstant;
import com.mashibing.base.BaseTest;
import com.mashibing.serializer.impl.UserSerializer;
import com.mashibing.serializer.pojo.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * description: com.mashibing.serializer
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/11/12
 * version: 1.0
 */
public class ProducerUser extends BaseTest {

    private static final Logger logger = LoggerFactory.getLogger(ProducerUser.class);

    public static final String USER_TOPIC = "user001";

    /**
     * description: 创建主题
     * create by: zhaosong 2024/11/12 11:20
     */
    @Before
    public void createTopic() {
        createTopic(USER_TOPIC, 3, (short) 2);
    }

    /**
     * description: 生产消息
     * create by: zhaosong 2024/11/12 11:21
     */
    @Test
    public void produce() {
        // 构建生产者连接/配置信息
        Properties props = initBaseConf();

        // 构建生产者对象
        try (KafkaProducer<String, User> producer = new KafkaProducer<>(props);) {
            // 构建消息实体
            ProducerRecord<String, User> record = new ProducerRecord<>(USER_TOPIC
                    , User.builder().id(1).name("test0001").build());

            // 发送消息
            Future<RecordMetadata> future = producer.send(record);
            // 阻塞
            RecordMetadata metadata = future.get();
            int partition = metadata.partition();
            long offset = metadata.offset();
            logger.info("topic: {}, key: {}, value: {}, partition: {}, offset: {}"
                    , record.topic(), record.key(), record.value(), partition, offset);
        } catch (Exception ex) {
            logger.error("Failed to send message!", ex);
        }
    }

    /**
     * 初始化生产者连接/配置信息
     * description:
     * create by: zhaosong 2024/11/12 11:22
     *
     * @return
     */
    private Properties initBaseConf() {
        Properties props = new Properties();
        // 连接信息
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BaseKafkaConstant.BOOT_SERVERS);
        // 序列化
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer.class.getName());
        return props;
    }
}
