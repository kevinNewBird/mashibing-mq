package com.mashibing.mq.demo.pubsub;

import com.mashibing.mq.constant.ExchangeConstant;
import com.mashibing.mq.util.RabbitMQConnectUtil;
import com.rabbitmq.client.*;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * description: 可靠消息发送(可参考confirms目录)
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/11/5
 * version: 1.0
 */
public class PublisherReliable {

    @Test
    public void publish() {
        // 1.建立连接和通道
        try (Connection conn = RabbitMQConnectUtil.buildConnection();
             Channel channel = conn.createChannel()) {
            // 2.创建交换机
            channel.exchangeDeclare(ExchangeConstant.PUBSUB.getExchangeName()
                    , ExchangeConstant.PUBSUB.getExchangeType());

            // 3.发送消息
            // 3.1.确保到exchange的可靠性,即confirm机制
            channel.confirmSelect();
            ConcurrentSkipListSet<Long> confirmSet = new ConcurrentSkipListSet<>(); // 确认消息列表
            ConfirmCallback ackConfirmCallback = (deliveryTag, multiple) -> {
                if (multiple) {
                    confirmSet.headSet(deliveryTag).clear();
                } else {
                    confirmSet.remove(deliveryTag);
                }
            };
            ConfirmCallback nackConfirmCallback = (deliveryTag, multiple) -> {
                if (multiple) {
                    confirmSet.headSet(deliveryTag + 1).clear();
                } else {
                    confirmSet.remove(deliveryTag);
                }
            };
            channel.addConfirmListener(ackConfirmCallback, nackConfirmCallback);
            // 3.2.确保exchange到队列的可靠性，即开启return机制
            ReturnCallback returnCallback = returnMessage -> {
                System.err.println("===============================ReturnListener===============================");
                System.err.println("replyCode: " + returnMessage.getReplyCode());
                System.err.println("replyText: " + returnMessage.getReplyText());
                System.err.println("exchange: " + returnMessage.getExchange());
                System.err.println("routingKey: " + returnMessage.getRoutingKey());
                System.err.println("properties: " + returnMessage.getProperties());
                System.err.println("body: " + new String(returnMessage.getBody(), StandardCharsets.UTF_8));
                System.err.println("===============================ReturnListener===============================");
            };
            channel.addReturnListener(returnCallback);
            // 设置发送消息的持久化
            AMQP.BasicProperties props = new AMQP.BasicProperties().builder()
                    .deliveryMode(2)
                    .build();

            for (int i = 0; i < 100; i++) {
                confirmSet.add(channel.getNextPublishSeqNo());
                // 开启return机制，必须开启mandatory=true(加上return机制才会生效)
                channel.basicPublish(ExchangeConstant.PUBSUB.getExchangeName(),
                        "", true, props, String.format("[reliable] [%s]Hello world!", i).getBytes());
            }

            channel.waitForConfirms();// 等待消息确认结束
        } catch (Exception ex) {
            System.err.println(String.format("通讯方式【%s】: 发送消息失败！", "pub/sub"));
            ex.printStackTrace();
        }
    }
}
