package com.mashibing.mq.demo.deadletter;

import com.mashibing.mq.constant.ExchangeConstant;
import com.mashibing.mq.constant.MessageConstant;
import com.mashibing.mq.util.RabbitMQConnectUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import org.junit.Test;
import sun.plugin2.message.Message;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * description: 死信队列
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/11/5
 * version: 1.0
 */
public class DeadLetterConsumer {

    @Test
    public void consume() {
        // 1.创建连接和通道
        try (Connection conn = RabbitMQConnectUtil.buildConnection();
             Channel channel = conn.createChannel()) {
            // 2.创建交互机（direct）
            channel.exchangeDeclare(ExchangeConstant.DEAD_LETTER.getExchangeName()
                    , ExchangeConstant.DEAD_LETTER.getExchangeType());

            channel.queueDeclare(MessageConstant.DEAD_QUEUE, false, false, false, null);
            channel.queueBind(MessageConstant.DEAD_QUEUE, ExchangeConstant.DEAD_LETTER.getExchangeName()
                    , MessageConstant.DEAD_ROUTING_KEY);
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                try {
                    String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                    System.out.println("[dead letter] received '" + message + "'");
                } finally {
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            };
            channel.basicConsume(MessageConstant.DEAD_QUEUE, false, deliverCallback
                    , consumerTag -> {
                        System.err.println("C2取消信息");
                    });
            // 阻塞，保证线程可以消费到
            System.in.read();
        } catch (Exception e) {
            System.err.println(String.format("通讯方式【%s】: 接收消息失败！", "direct"));
            e.printStackTrace();
        }
    }
}
