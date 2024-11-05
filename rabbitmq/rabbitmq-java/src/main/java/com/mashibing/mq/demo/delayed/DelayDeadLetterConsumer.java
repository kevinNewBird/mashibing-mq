package com.mashibing.mq.demo.delayed;

import com.mashibing.mq.constant.ExchangeConstant;
import com.mashibing.mq.constant.MessageConstant;
import com.mashibing.mq.util.RabbitMQConnectUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * description: 死信队列
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/11/5
 * version: 1.0
 */
public class DelayDeadLetterConsumer {

    @Test
    public void consume() {
        // 1.创建连接和通道
        try (Connection conn = RabbitMQConnectUtil.buildConnection();
             Channel channel = conn.createChannel()) {
            // 2.创建交互机（direct）
            channel.exchangeDeclare(ExchangeConstant.DELAYED_DEAD_LETTER.getExchangeName()
                    , ExchangeConstant.DELAYED_DEAD_LETTER.getExchangeType());

            channel.queueDeclare(MessageConstant.DELAYED_DEAD_QUEUE, false, false, false, null);
            channel.queueBind(MessageConstant.DELAYED_DEAD_QUEUE, ExchangeConstant.DELAYED_DEAD_LETTER.getExchangeName()
                    , MessageConstant.DELAYED_DEAD_ROUTING);
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                try {
                    String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                    System.out.println("[dead letter] received '" + message + "'");
                } finally {
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            };
            channel.basicConsume(MessageConstant.DELAYED_DEAD_QUEUE, false, deliverCallback
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
