package com.mashibing.complex;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * description: com.mashibing.complex
 * company: 北京海量数据有限公司
 * create by: zhaosong 2023/4/12
 * version: 1.0
 */
public class ComplexKafkaProducer {

    private static final Logger logger = LoggerFactory.getLogger(ComplexKafkaProducer.class);


    private static Properties initConf() {
        Properties conf = new Properties();

        conf.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.12:9092");

        conf.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        conf.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        conf.setProperty(ProducerConfig.ACKS_CONFIG, "0");
        conf.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class.getName());

        // 默认是16k = 16384，即ProducerBatch
        // 数据不是立刻发送出去，而是会由RecordAccumulator累加器尝试往双边队列的最后一个ProducerBatch中累加
        // 考虑如果一个消息大于batch(假设默认16k), 那么就会申请一个临时空间，用完就回收
        // 所以实际中要评估消息的大小，保证一个batch可能放下尽可能多的数据
        //调整目的：减少内存碎片和系统调用复杂度
        conf.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        // 发送消息会有两种形式：
        // 1.阻塞，一条发送然后阻塞获取确认信息
        // 2.非阻塞的，但使用这种模式会和LINGER_MS_CONFIG相关
        // 而阻塞与否，又和ack的值有关
        /**
         * LINGER_MS_CONFIG:
         * 0表示：不需要计时等待，一条一条的发送，和batch无关
         * >0表示：io thread线程获取到batch，计时等待30s之后或者batch满了才发送。
         *    牵扯到生产和io速度不对称，这个时候就会由消息累积到batch
         */
        conf.setProperty(ProducerConfig.LINGER_MS_CONFIG, "0");

        // message.max.bytes，默认是 1M
        // 这个值决定了最终发送数据的包大小，必须大于batch_size_config的大小，也就是说batch_size也可以不调整
        // ，该值也非常重要，决定了最终的发送时机（受服务器上的配置影响）
        conf.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576");

        // 默认是32M（如果有很多topic, 可能就会有每个topic所属的空间里只有一条会几条数据，就导致32M空间打满
        // ，所以该值应该依据实际生产情况进行调整）, 这个指定的是RecordAccumulator累加器的内存大小，如果满了
        // ，后续再发送消息将会阻塞。会一直阻塞吗？
        conf.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        // 当累加器内存满了，由这个控制，默认是阻塞60s，如果超过60s还是消费不完，将会抛异常
        conf.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "6000");


        // 飞行队列,发送了5次，但是kafka服务端没有返回，最大积压5个飞行请求，超过后，剩下的请求就不能发了
        // 也就是说，数据打包后，首先会放入一个飞行队列中，然后走nioSelector发送逻辑
        conf.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // 涉及tcp侧，可以配置成 -1，表示使用操作系统的默认值（cat /proc/sys/net/core/wmem_max）
        conf.setProperty(ProducerConfig.SEND_BUFFER_CONFIG, "32768"); // 默认32k，内核的发送数据的缓存区大小
        // 涉及tcp侧，可以配置成 -1，表示使用操作系统的默认值（cat /proc/sys/net/core/rmem_max）
        conf.setProperty(ProducerConfig.RECEIVE_BUFFER_CONFIG, "32768"); // 默认32k，内核的接收数据的缓存区大小


        return conf;
    }

    @Test
    public void producer() {
        Properties config = initConf();
        // produer构造器： 主要初始化了消息累加器、IO Thread发送线程
        try (Producer<String, String> producer = new KafkaProducer<String, String>(config);) {

            ProducerRecord<String, String> record = new ProducerRecord<>(ComplexKafkaConstant.COMPLEX_TOPIC, "hello", "complex val");
            Future<RecordMetadata> ft = producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                }
            });

            // 阻塞
            RecordMetadata meta = ft.get();


        } catch (Throwable ex) {
            logger.error("发送消息发生错误！", ex);
        }

    }
}
