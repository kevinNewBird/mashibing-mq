package com.mashibing.commit;

import com.mashibing.base.BaseKafkaConstant;
import com.mashibing.base.BaseTest;
import com.mashibing.pojo.User;
import com.mashibing.serializer.impl.UserDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * description: com.mashibing.commit
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/11/12
 * version: 1.0
 */
public class ConsumerManualCommit extends BaseTest {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerManualCommit.class);

    private static final String GROUP_ID = "group01";

    /**
     * description: 创建主题
     * create by: zhaosong 2024/11/12 11:20
     */
    @Before
    public void createTopic() {
        createTopic(ProducerManualCommit.USER_TOPIC, 3, (short) 2);
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
        KafkaConsumer<String, User> consumer = new KafkaConsumer<>(props);
        try {
            // 订阅消费TOPIC
            consumer.subscribe(Collections.singleton(ProducerManualCommit.USER_TOPIC));
            // 循环拉取信息（监听）
            while (true) {
                // 每隔2秒，拉取准备就绪的信息列表
                ConsumerRecords<String, User> recordList = consumer.poll(Duration.ofSeconds(2));
                if (recordList.isEmpty()) {
                    continue;
                }

                // 模拟消息消费, 并手动提交ack
                // 1.1.批次手动提交
//                consumeBatchByAsync(consumer, recordList);
                // 1.2.根据分区提交
                commitPartitionByAsync(consumer, recordList);
            }
        } catch (Exception ex) {
            logger.error("Failed to receive message!", ex);
        } finally {
            // 同步提交，确保发生异常时，能成功的提交ack（一定会成功，失败内部会重试）
            consumer.commitAsync();
            // 释放客户端资源
            consumer.close();
        }
    }

    /**
     * 根据消息数量，异步提交ack（可能会失败）
     * description:
     * create by: zhaosong 2024/11/12 17:20
     */
    private void consumeBatchByAsync(KafkaConsumer<String, User> consumer, ConsumerRecords<String, User> recordList) {
        // 模拟消息消费, 并手动提交ack
        // 每隔多少消息提交ack
        int count = 0;
        Map<TopicPartition, OffsetAndMetadata> ackMap = new HashMap<>();
        // 不适用使用多线程的方式提交（串行方式）
        for (ConsumerRecord<String, User> record : recordList) {
            int partition = record.partition();
            long offset = record.offset();
            logger.info("topic: {},key: {}, value: {}, partition: {}, offset: {}"
                    , record.topic(), record.key(), record.value(), partition, offset);
            ackMap.put(new TopicPartition(record.topic(), record.partition())
                    , new OffsetAndMetadata(record.offset()));
            if (++count % 10 == 0) {
                consumer.commitAsync(ackMap, (Map<TopicPartition, OffsetAndMetadata> offsets, Exception ex) -> {
                    if (null == ex) {
                        logger.info("commit success!");
                    } else {
                        logger.error("Failed to commit ack!", ex);
                    }
                });
                ackMap.clear();
            }
        }
        // 确保数据不足10条时，提交本次
        if (!ackMap.isEmpty()) {
            consumer.commitAsync(ackMap, (offsets, ex) -> {
                if (null == ex) {
                    logger.info("commit success!");
                } else {
                    logger.error("Failed to commit ack!", ex);
                }
            });
        }
    }

    /**
     * 根据分区，异步提交ack(可能会失败)
     */
    private void commitPartitionByAsync(KafkaConsumer<String, User> consumer, ConsumerRecords<String, User> recordList) {
        Set<TopicPartition> partitions = recordList.partitions();
        // 不同分区的异步提交是不干扰的，可使用并行方式提交ack
        // 需要注意的是：拉取之间的手动ack要保持串行
        for (TopicPartition topicPartition : partitions) {
            Map<TopicPartition, OffsetAndMetadata> ackMap = new HashMap<>();
            // 获取分区内的消息列表
            List<ConsumerRecord<String, User>> records = recordList.records(topicPartition);
            for (ConsumerRecord<String, User> record : records) {
                logger.info("topic: {},key: {}, value: {}, partition: {}, offset: {}"
                        , record.topic(), record.key(), record.value(), record.partition(), record.offset());
            }

            // 分区：手动ack
            long offset = records.get(records.size() - 1).offset();
            ackMap.put(topicPartition, new OffsetAndMetadata(offset));
            consumer.commitAsync(ackMap, (offsets, ex) -> {
                if (null == ex) {
                    logger.info("Success to commit!");
                } else {
                    logger.error("Failed to commit ack!", ex);
                }
            });
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

        // 关闭自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // 指定消费的规则
        // 第一次连接成功，消费数据从那开始：默认latest从末尾消费，earliest从头部消费，none没有找到抛异常
        // 查看分组列表：kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
        /**
         * 查看指定分组的消费信息：kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe
         *
         * TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
         * simple-s001     1          23              26              3               -               -               -
         * simple-s001     0          44              50              6               -               -               -
         *
         * tip: 上述，分区1当前的offset为23，记录的最后的offset为26，LAG=3表示3个未消费；分区0同理
         *
         * 结合上述的信息(新建或已有的所有分组的current-offset,log-end-offset都是一致的)
         * latest: 表示该分组第一次连接进入后，将会将当前分组的current-offset更新为log-end-offset, 这将导致未消费的数据丢失没有被消费到（只针对当前分组）
         *         如果该分组已经被连接过了，已经消费过的offset不再进行消费，消费后续进入的新的未被消费的offset，举例--分区1的23、24、25，分区0的44、46...49被消费，以及后续的新的消息
         *
         * earliest: 表示该分组第一次连接进入后，从头开始消费，举例--分区1的0-26，分区0的0-50，以及后续的新消息；
         *           如果该分组已经被连接过了，那么只会消费未被消费过的数据，即分区1的23、24、25，分区0的44、46...49被消费，以及后续的新的消息
         *
         * none: 如果该分组没有存在则抛出异常
         *
         * 注：latest和earliest选项分组不存在将自动创建
         */
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}
