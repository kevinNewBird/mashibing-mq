package com.mashibing.mq.demo.deadletter;

import com.mashibing.mq.constant.ExchangeConstant;
import com.mashibing.mq.constant.MessageConstant;
import com.mashibing.mq.util.RabbitMQConnectUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * description: 普通队列信息消费
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/11/5
 * version: 1.0
 */
public class GeneralConsumer {

    @Test
    public void consume() {
        // 1.创建连接和通道
        try (Connection conn = RabbitMQConnectUtil.buildConnection();
             Channel channel = conn.createChannel()) {
            // 2.创建或声明普通队列和死信队列
            // 2.1.普通队列
            channel.exchangeDeclare(ExchangeConstant.DEAD_GENERAL.getExchangeName(), ExchangeConstant.DEAD_GENERAL.getExchangeType());
            // 2.2.死信队列
            channel.exchangeDeclare(ExchangeConstant.DEAD_LETTER.getExchangeName(), ExchangeConstant.DEAD_LETTER.getExchangeType());

            // 2.3.创建死信队列
            channel.queueDeclare(MessageConstant.DEAD_QUEUE, false, false, false, null);
            channel.queueBind(MessageConstant.DEAD_QUEUE, ExchangeConstant.DEAD_LETTER.getExchangeName(), MessageConstant.DEAD_ROUTING_KEY);

            // 3.声明普通队列
            Map<String, Object> arguments = new HashMap<>();
            // 3.1.过期时间
            arguments.put("x-message-ttl", 10000);
            // 3.2.正常队列设置死信交换机
            arguments.put("x-dead-letter-exchange", ExchangeConstant.DEAD_LETTER.getExchangeName());
            // 3.3.设置死信routing key
            arguments.put("x-dead-letter-routing-key", MessageConstant.DEAD_ROUTING_KEY);
            // 3.4.设置正常队列长度限制
            arguments.put("x-max-length", 10);

            channel.queueDeclare(MessageConstant.DEAD_GENERAL_QUEUE, false, false, false, arguments);
            channel.queueBind(MessageConstant.DEAD_GENERAL_QUEUE, ExchangeConstant.DEAD_GENERAL.getExchangeName()
                    , MessageConstant.DEAD_GENERAL_ROUTING);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                try {
                    String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

                    if (StringUtils.containsIgnoreCase(message, "[5]")) {
                        System.out.println("[dead letter] reject '" + message + "'");
                        // 拒绝消息
                        channel.basicReject(delivery.getEnvelope().getDeliveryTag(), false);
                    } else {
                        System.out.println("[dead letter] received '" + message + "'");
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            };

            channel.basicConsume(MessageConstant.DEAD_GENERAL_QUEUE, false, deliverCallback, consumerTag -> {
                System.err.println("C1取消信息");
            });
            // 阻塞，保证线程可以消费到
            System.in.read();
        } catch (Exception e) {
            System.err.println(String.format("通讯方式【%s】: 接收消息失败！", "direct"));
            e.printStackTrace();
        }
    }
}
