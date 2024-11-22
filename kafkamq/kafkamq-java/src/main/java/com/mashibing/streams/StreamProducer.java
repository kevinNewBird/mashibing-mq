package com.mashibing.streams;

import com.mashibing.base.BaseKafkaConstant;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

/**
 * 类说明：流数据的生产者（最原始的数据）
 */
public class StreamProducer {

    private static KafkaProducer<String, String> producer = null;

    public static final String PRODUCT_TOPIC = "sell";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BaseKafkaConstant.BOOT_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        /*消息生产者*/
        producer = new KafkaProducer<String, String>(properties);
        String[] words = {"iphone", "huawei", "xiaomi", "oppo", "vivo"};
        Random r = new Random();
        Random r1 = new Random();
        try {
            ProducerRecord<String, String> record;
            /*待发送的消息实例*/
            while (true) {
                int wordCount = r.nextInt(5);
                if (wordCount == 0) continue;
                StringBuilder sb = new StringBuilder("");
                for (int i = 0; i < wordCount; i++) {
                    sb.append(words[r1.nextInt(words.length)]).append(" ");
                }
                try {
                    record = new ProducerRecord<String, String>(PRODUCT_TOPIC, "product", sb.toString());
                    producer.send(record);
                    System.out.println("购买的商品为：" + sb.toString());
                    Thread.sleep(2);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } finally {
            producer.close();
        }
    }


}
