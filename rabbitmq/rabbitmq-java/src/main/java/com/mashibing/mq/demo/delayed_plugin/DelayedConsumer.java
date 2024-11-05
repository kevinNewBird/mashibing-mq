package com.mashibing.mq.demo.delayed_plugin;

import com.mashibing.mq.constant.ExchangeConstant;
import com.mashibing.mq.util.RabbitMQConnectUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * description: 普通队列信息消费
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/11/5
 * version: 1.0
 */
public class DelayedConsumer {

    @Test
    public void consume() {
        // 1.创建连接和通道
        try (Connection conn = RabbitMQConnectUtil.buildConnection();
             Channel channel = conn.createChannel()) {
            // 2.创建交换机
            Map<String, Object> arguments = new HashMap<>();
            // 2.1.交换机类型
            arguments.put("x-delayed-type", "direct");
            channel.exchangeDeclare(ExchangeConstant.THRD_DELAYED.getExchangeName(), ExchangeConstant.THRD_DELAYED.getExchangeType()
                    , true, false, arguments);

            /// 创建一个延时队列
            Map<String, Object> queueArgs = new HashMap<>();
            queueArgs.put("x-dead-letter-exchange", "");
            queueArgs.put("x-dead-letter-routing-key", DelayedPublisher.ROUTING_KEY);
            queueArgs.put("x-message-ttl", 30000);
            channel.queueDeclare(DelayedPublisher.QUEUE_NAME, true, false, false, queueArgs);
            channel.queueBind(DelayedPublisher.QUEUE_NAME, ExchangeConstant.THRD_DELAYED.getExchangeName(), DelayedPublisher.ROUTING_KEY);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message;
                try {
                    message = new String(delivery.getBody(), "utf-8");
                    System.out.println("[delayed] received '" + message + "'");
                } finally {
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            };

            // 死信生效的条件:设置autoAck=false，且在deliverCallback调用拒绝basicReject
            channel.basicConsume(DelayedPublisher.QUEUE_NAME, false, deliverCallback, consumerTag -> {
                System.err.println("取消信息");
            });
            // 阻塞，保证线程可以消费到
            System.in.read();
        } catch (Exception e) {
            System.err.println(String.format("通讯方式【%s】: 接收消息失败！", "direct"));
            e.printStackTrace();
        }
    }
}
