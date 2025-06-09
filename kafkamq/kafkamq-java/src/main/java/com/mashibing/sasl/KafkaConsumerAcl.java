package com.mashibing.sasl;

import com.mashibing.simple.SimpleKafkaProducer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

import static com.mashibing.base.BaseKafkaConstant.BOOT_SERVERS_ACL;

/**
 * description: com.mashibing.simple
 * company: 北京海量数据有限公司
 * create by: zhaosong 2023/3/29
 * version: 1.0
 */
public class KafkaConsumerAcl {

    private static Logger logger = LoggerFactory.getLogger(SimpleKafkaProducer.class);

    // 参考链接：https://www.cnblogs.com/jiaxzeng/p/17219061.html
    // 参考链接2: https://www.cnblogs.com/route/p/18783755
    /**
     * description: 自动提交的情况，简单处理分区数据
     * create by: zhaosong 2023/4/3 21:08
     */
    @Test
    public void consumer0() {
        Properties config = new Properties();
        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  BOOT_SERVERS_ACL);
        // 编解码
        config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //  用户名密码
        config.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"adminsecret\";");
        //  sasl
        config.put("security.protocol", "SASL_PLAINTEXT");
        //   sasl
        config.put("sasl.mechanism", "SCRAM-SHA-256");

        // 消费的细节(消费指定的组)
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group010");
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
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // 开启自动提交，默认值为true(也就是数据消费后，是自动去维护offset，还是手动去维护offset)
        // 自动提交是异步提交，数据拉回来不是立刻就去更新offset,而是有个间隔（默认5s），也就是ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG
        // 这样就会存在问题，数据被消费了，但是offset还没有被提交，那么就有可能会有丢数据和重复消费数据的情况发生
        config.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        /// 1，还没到时间，挂了，没提交，重起个consuemr，参照offset的时候，会重复消费
        // 2，个批次的数据还没写数据库成功，但是这个批次的offset背异步提交了，挂了，重起一个consuemr，参照offset的时候，会丢失消息
        config.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");

        // poll拉取数据，弹性，按需，拉取多少
//        config.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"");

        // 注：构建的Consumer的泛型和前面的key和value的反序列化类型式对应的。
        try (Consumer<String, String> consumer = new KafkaConsumer<String, String>(config);) {

            // 订阅topic
            consumer.subscribe(Collections.singletonList(KafkaProducerAcl.SIMPLE_TOPIC), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                    System.out.println("----------分区撤回---------");
                    collection.forEach(p -> {
                        System.out.println(p.partition());
                    });
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                    System.out.println("----------分区分配---------");
                    collection.forEach(p -> {
                        System.out.println(p.partition());
                    });
                }
            });


            while (true) {
                // 拉取小批次的数据(0表示一直阻塞直到拉取到数据，>0表示超过该时间不在阻塞获取)
                // （！！！）思考：当只有一个consumer时，订阅了2个分区，那么这个批次数据是怎么拉取呢？
                // 结论：见consumer1()
                // 每隔2s从broker获取消息
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2));

                // 以下代码的优化很重要

                if (records.isEmpty()) {
                    continue;
                }
                logger.info("----------------消息小批次数量：{}----------------", records.count());

                Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
                while (iterator.hasNext()) {
                    ConsumerRecord<String, String> record = iterator.next();
                    int partition = record.partition();
                    long offset = record.offset();
                    logger.info("topic: {},key: {}, val: {}, partition: {}, offset: {}"
                            , record.topic(), record.key(), record.value(), partition, offset);
                }
            }
        }

    }
}
