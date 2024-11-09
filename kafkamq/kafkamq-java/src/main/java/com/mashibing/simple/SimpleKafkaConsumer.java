package com.mashibing.simple;

import com.mashibing.base.BaseKafkaConstant;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

import static com.mashibing.simple.SimpleKafkaConstant.SIMPLE_TOPIC;

/**
 * description: com.mashibing.simple
 * company: 北京海量数据有限公司
 * create by: zhaosong 2023/3/29
 * version: 1.0
 */
public class SimpleKafkaConsumer {

    private static Logger logger = LoggerFactory.getLogger(SimpleKafkaProducer.class);

    /**
     * description: 自动提交的情况，简单处理分区数据
     * create by: zhaosong 2023/4/3 21:08
     */
    @Test
    public void consumer0() {
        Properties config = new Properties();
        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BaseKafkaConstant.BOOT_SERVERS);
        // 编解码
        config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

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
            consumer.subscribe(Collections.singletonList(SIMPLE_TOPIC), new ConsumerRebalanceListener() {
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

    /**
     * description: 手动提交，并自行控制提交逻辑
     * create by: zhaosong 2023/4/3 21:08
     */
    @Test
    public void consumer1() {
        Properties config = new Properties();
        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BaseKafkaConstant.BOOT_SERVERS);
        // 编解码
        config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 消费的细节(消费指定的组)
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group010");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 关闭自动提交
        config.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // poll拉取数据，弹性，按需，拉取多少
//        config.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"");

        // 注：构建的Consumer的泛型和前面的key和value的反序列化类型式对应的。
        try (Consumer<String, String> consumer = new KafkaConsumer<String, String>(config);) {

            // 订阅topic
            consumer.subscribe(Collections.singletonList(SIMPLE_TOPIC), new ConsumerRebalanceListener() {
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
                // 结论：拉取多个分区的数据，看下面的records.partitions()即可得出该结论
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));

                // 以下代码的优化很重要

                if (records.isEmpty()) {
                    continue;
                }
                logger.info("----------------消息小批次数量：{}----------------", records.count());
                // 每次poll的时候是取多个分区的数据
                // 且每个分区内的数据是有序的
                Set<TopicPartition> partitions = records.partitions();


                /**
                 * 如果手动提交offset，有如下三种方式：
                 * 1.按消息进度同步提交
                 * 2.按分区粒度同步提交
                 * 3.按当前poll的批次同步提交
                 *
                 * 思考：如果在多个线程下
                 * 1.以上1，3的方式不用多线程
                 * 2.以上2的方式最容易想到多线程方式处理，有没有问题？前提是使用一个job控制，job是串行的，是没有问题的。
                 */
                for (TopicPartition partition : partitions) {
                    // 单独获取一个分区的数据
                    List<ConsumerRecord<String, String>> pRecords = records.records(partition);
                    //在一个微批里，按分区获取poll回来的数据
                    //线性按分区处理，还可以并行按分区处理用多线程的方式
                    Iterator<ConsumerRecord<String, String>> iterator = pRecords.iterator();
                    while (iterator.hasNext()) {
                        ConsumerRecord<String, String> record = iterator.next();
                        int par = record.partition();
                        long offset = record.offset();
                        logger.info("topic: {},key: {}, val: {}, partition: {}, offset: {}"
                                , record.topic(), record.key(), record.value(), par, offset);

                        // 第一种提交方式：按单条消息
                        /*TopicPartition tPar = new TopicPartition(SIMPLE_TOPIC, par);
                        OffsetAndMetadata om = new OffsetAndMetadata(offset);
                        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                        map.put(tPar, om);
                        consumer.commitSync(map);*/
                    }
                    // 第二种方式：按分区处理
                    /**
                     * 因为你都分区了，拿到了分区的数据集，期望的是先对数据整体加工
                     * 小问题会出现？你怎么知道最后一条小的offset?
                     */
                   /* long pOffset = pRecords.get(pRecords.size() - 1).offset();
                    OffsetAndMetadata pom = new OffsetAndMetadata(pOffset);
                    Map<TopicPartition, OffsetAndMetadata> pmap = new HashMap<>();
                    pmap.put(partition, pom);
                    consumer.commitSync(pmap);*/

                }

                // 第三种提交方式: 按poll拉取的当前批次处理
                consumer.commitSync();
            }
        }

    }


    /**
     * description: 根据时间戳消费数据
     * create by: zhaosong 2023/4/10 9:24
     */
    @Test
    public void consumer2() {
        Properties config = new Properties();
        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BaseKafkaConstant.BOOT_SERVERS);
        // 编解码
        config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 消费的细节(消费指定的组)
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group010");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 关闭自动提交
        config.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // poll拉取数据，弹性，按需，拉取多少
//        config.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"");

        // 注：构建的Consumer的泛型和前面的key和value的反序列化类型式对应的。
        try (Consumer<String, String> consumer = new KafkaConsumer<String, String>(config);) {

            // 订阅topic
            consumer.subscribe(Collections.singletonList(SIMPLE_TOPIC), new ConsumerRebalanceListener() {
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

            // <-----------------******************(!!!)根据时间戳获取对应的offset******************--------------------->
            /**
             * 以下代码是你在未来开发的时候，向通过自定时间点的方式，自定义消费数据位置
             *
             * 其实本质，核心知识是seek 方法
             * 举一反三：
             * 1、通过时间换算出offset，再通过seek来自定义偏移
             * 2、如果你自己维护offset持久化~! ! !
             */

            // 通过consumer取回自己分配的分区
            Set<TopicPartition> as = consumer.assignment();

            Map<TopicPartition, Long> tts = new HashMap<>();
            // （****）这里需要明确一点，consumer在poll拉取数据的时候才会去建立连接
            while (as.isEmpty()) {
                consumer.poll(Duration.ofMillis(100));
                as = consumer.assignment();
            }

            // 自己填充一个hashmap，为每个分区设置对应的时间戳
            for (TopicPartition partition : as) {
                // 这个时间戳不易过小，否则可能会导致后面的offset获取不到,所以在后续需要对换算出来的offset做判断
                tts.put(partition, System.currentTimeMillis() - 1 * 1000);
            }

            // 通过consumer的api，取回timeindex的数据
            Map<TopicPartition, OffsetAndTimestamp> offsetTime = consumer.offsetsForTimes(tts);

            for (TopicPartition partition : as) {
                // 通过取回的offset数据，通过consumer的seek方法，修正自己的消费偏移
                OffsetAndTimestamp offsetAndTimestamp = offsetTime.get(partition);
                long offset = offsetAndTimestamp.offset();// 如果不是通过time换offset, 而是通过mysql读取回来，其本质是一样的（通过seek）
                // 修正到时间戳对应的offset
                consumer.seek(partition, offset);
            }
            // <------------------******************(!!!)根据时间戳获取对应的offset******************-------------------->

            while (true) {
                // 拉取小批次的数据(0表示一直阻塞直到拉取到数据，>0表示超过该时间不在阻塞获取)
                // （！！！）思考：当只有一个consumer时，订阅了2个分区，那么这个批次数据是怎么拉取呢？
                // 结论：拉取多个分区的数据，看下面的records.partitions()即可得出该结论
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));

                // 以下代码的优化很重要

                if (records.isEmpty()) {
                    continue;
                }
                logger.info("----------------消息小批次数量：{}----------------", records.count());
                // 每次poll的时候是取多个分区的数据
                // 且每个分区内的数据是有序的
                Set<TopicPartition> partitions = records.partitions();


                /**
                 * 如果手动提交offset，有如下三种方式：
                 * 1.按消息进度同步提交
                 * 2.按分区粒度同步提交
                 * 3.按当前poll的批次同步提交
                 *
                 * 思考：如果在多个线程下
                 * 1.以上1，3的方式不用多线程
                 * 2.以上2的方式最容易想到多线程方式处理，有没有问题？前提是使用一个job控制，job是串行的，是没有问题的。
                 */
                for (TopicPartition partition : partitions) {
                    // 单独获取一个分区的数据
                    List<ConsumerRecord<String, String>> pRecords = records.records(partition);
                    //在一个微批里，按分区获取poll回来的数据
                    //线性按分区处理，还可以并行按分区处理用多线程的方式
                    Iterator<ConsumerRecord<String, String>> iterator = pRecords.iterator();
                    while (iterator.hasNext()) {
                        ConsumerRecord<String, String> record = iterator.next();
                        int par = record.partition();
                        long offset = record.offset();
                        long timestamp = record.timestamp();
                        logger.info("topic: {},key: {}, val: {}, partition: {}, offset: {}, timestamp: {}"
                                , record.topic(), record.key(), record.value(), par, offset, timestamp);

                        // 第一种提交方式：按单条消息
                        /*TopicPartition tPar = new TopicPartition(SIMPLE_TOPIC, par);
                        OffsetAndMetadata om = new OffsetAndMetadata(offset);
                        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                        map.put(tPar, om);
                        consumer.commitSync(map);*/
                    }
                    // 第二种方式：按分区处理
                    /**
                     * 因为你都分区了，拿到了分区的数据集，期望的是先对数据整体加工
                     * 小问题会出现？你怎么知道最后一条小的offset?
                     */
                   /* long pOffset = pRecords.get(pRecords.size() - 1).offset();
                    OffsetAndMetadata pom = new OffsetAndMetadata(pOffset);
                    Map<TopicPartition, OffsetAndMetadata> pmap = new HashMap<>();
                    pmap.put(partition, pom);
                    consumer.commitSync(pmap);*/

                }

                // 第三种提交方式: 按poll拉取的当前批次处理
                consumer.commitSync();
            }
        }

    }
}
