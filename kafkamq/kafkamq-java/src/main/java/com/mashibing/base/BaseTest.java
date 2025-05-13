package com.mashibing.base;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

import java.util.Collections;
import java.util.Properties;

/**
 * description: com.mashibing.base
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/11/12
 * version: 1.0
 */
public abstract class BaseTest {

    /**
     * 创建主题
     * description:
     * create by: zhaosong 2024/11/12 11:12
     *
     * @param topic
     * @param numPartitions: 分区数
     * @param replications:  副本数
     */
    protected void createTopic(String topic, int numPartitions, short replications) {
        createTopic(false, topic, numPartitions, replications);
    }

    /**
     * 创建主题
     * description:
     * create by: zhaosong 2024/11/12 11:12
     *
     * @param topic
     * @param numPartitions: 分区数
     * @param replications:  副本数
     */
    protected void createTopic(boolean isAcl, String topic, int numPartitions, short replications) {
        // 配置kafka连接信息
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, isAcl ? BaseKafkaConstant.BOOT_SERVERS_ACL : BaseKafkaConstant.BOOT_SERVERS);
        if (isAcl) {
            //  用户名密码
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"ops\" password=\"ops-secret\";");
            //  sasl
            props.put("security.protocol", "SASL_PLAINTEXT");
            //   sasl
            props.put("sasl.mechanism", "SCRAM-SHA-512");
        }
        try (AdminClient client = AdminClient.create(props)) {
            // 判断topic是否已经存在
            KafkaFuture<Boolean> isExistFuture = client.listTopics().names().thenApply(topics -> topics.contains(topic));
            // 主题已存在，不继续执行
            if (isExistFuture.get()) {
                return;
            }
            // 创建主题，并阻塞等待其完成
            NewTopic newTopic = new NewTopic(topic, numPartitions, replications);
            client.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
