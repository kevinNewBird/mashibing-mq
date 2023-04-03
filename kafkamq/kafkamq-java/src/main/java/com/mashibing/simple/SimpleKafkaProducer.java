package com.mashibing.simple;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.mashibing.simple.SimpleKafkaConstant.SIMPLE_TOPIC;

/**
 * description: com.mashibing.simple
 * company: 北京海量数据有限公司
 * create by: zhaosong 2023/3/29
 * version: 1.0
 */
public class SimpleKafkaProducer {

    private static Logger logger = LoggerFactory.getLogger(SimpleKafkaProducer.class);


    /**
     * description: kafka生产者
     * 约束：
     * 操作系统：centos7 x86_64
     * ip: 192.168.1.12
     * kafka版本：单机kafka
     * steps:
     * 1、创建topic
     * kafka-topics.sh --create --zookeeper localhost:2181 --partitions 2 --replication-factor 1 --topic simple-s001
     * <p>
     * create by: zhaosong 2023/3/29 9:14
     */
    @Test
    public void producer() throws ExecutionException, InterruptedException {
        // 配置kafka生产者
        Properties config = new Properties();
        // 注：确保服务器的防火墙关闭
        config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.12:9092");

        //kafka 持久化数据的MQ 数据-> byte[]，不会对数据进行干预，双方要约定编解码
        //kafka是一个app: : 使用零拷贝sendfile 系统调用实现快速数据消费
        config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 指定自己的分区器
//        config.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, SimplePartitioner.class.getName());
        Producer<String, String> producer = new KafkaProducer<>(config);

        ////现在的producer就是一个提供者，面向的其实是BFker，虽然在使用的时候我们期望把数据打入topic
        IntStream.range(0, 3).forEach(i -> {
            IntStream.range(0, 3).forEach(j -> {
                try {
                    ProducerRecord<String, String> record = new ProducerRecord<String, String>(SIMPLE_TOPIC, "key" + j, "val" + j + i);
                    // 异步,默认走的是hash分区器
                    Future<RecordMetadata> future = producer.send(record);
                    // 阻塞
                    RecordMetadata rm = future.get();
                    int partition = rm.partition();
                    long offset = rm.offset();
                    logger.info("topic: {},key: {}, val: {}, partition: {}, offset: {}"
                            , record.topic(), record.key(), record.value(), partition, offset);
                } catch (InterruptedException | ExecutionException e) {
                    logger.error("[TOPIC:simple-s001]发送消息发生错误！", e);
                }
            });
        });

        // 关闭连接
        producer.close();

    }
}
