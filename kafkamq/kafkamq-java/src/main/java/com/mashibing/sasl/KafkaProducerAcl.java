package com.mashibing.sasl;

import com.mashibing.base.BaseKafkaConstant;
import com.mashibing.base.BaseTest;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

/**
 * description: com.mashibing.simple
 * company: 北京海量数据有限公司
 * create by: zhaosong 2023/3/29
 * version: 1.0
 */
public class KafkaProducerAcl extends BaseTest {

    public static String SIMPLE_TOPIC = "test2";

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerAcl.class);

    @Before
    public void createTopic() {
        createTopic(true, SIMPLE_TOPIC, 1, (short) 1);
    }

    // 参考链接：https://www.cnblogs.com/jiaxzeng/p/17219061.html
    // 参考链接2: https://www.cnblogs.com/route/p/18783755

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
        config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BaseKafkaConstant.BOOT_SERVERS_ACL);

        //kafka 持久化数据的MQ 数据-> byte[]，不会对数据进行干预，双方要约定编解码
        //kafka是一个app: : 使用零拷贝sendfile 系统调用实现快速数据消费
        config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //  用户名密码
        config.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"adminsecret\";");
        //  sasl
        config.put("security.protocol", "SASL_PLAINTEXT");
        //   sasl
        config.put("sasl.mechanism", "SCRAM-SHA-256");
        /**
         * 为确保消息发送的可靠性，提供了acks属性，关于acks取值的说明（0,1,-1）:
         * 0: 生产者只管发送数据，即发送出去就认为是成功，缺点是：极有可能丢失数据（控制台打印的offset恒等于-1）
         * 1: （默认值）生产者发送数据，只需需接收到leader的确认信号，其余broker到leader同步数据，解决了丢失数据
         *      ，缺点是：效率会下降，在这里就会有几个节点的数据是不一致的，这就有了高水位和低水位的引出
         * -1： 用于分布式，强调的是备机的同步（主机可能挂掉），需接收到ISR集合中的所有的确认信号，这个是三者中最严苛的。
         */
//        config.setProperty(ProducerConfig.ACKS_CONFIG, "0");
        // 指定自己的分区器
//        config.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, SimplePartitioner.class.getName());

        // 注：构建的Producer的泛型和前面的key和value的序列化类型式对应的。
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
                    logger.info("topic: {},key: {}, val: {}, partition: {}, offset: {}", record.topic(), record.key(), record.value(), partition, offset);
                } catch (InterruptedException | ExecutionException e) {
                    logger.error("[TOPIC:simple-s001]发送消息发生错误！", e);
                }
            });
        });

        // 关闭连接
        producer.close();

    }
}
