package com.mashibing.mq.demo.delayed;

import com.mashibing.mq.constant.ExchangeConstant;
import com.mashibing.mq.constant.MessageConstant;
import com.mashibing.mq.util.RabbitMQConnectUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import org.junit.Test;

import java.time.LocalDateTime;
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
            // 2.创建或声明普通队列和死信队列
            // 2.1.普通队列
            channel.exchangeDeclare(ExchangeConstant.DELAYED.getExchangeName(), ExchangeConstant.DELAYED.getExchangeType());
            // 2.2.死信队列
            channel.exchangeDeclare(ExchangeConstant.DELAYED_DEAD_LETTER.getExchangeName(), ExchangeConstant.DELAYED_DEAD_LETTER.getExchangeType());

            // 2.3.创建死信队列
            channel.queueDeclare(MessageConstant.DELAYED_DEAD_QUEUE, false, false, false, null);
            channel.queueBind(MessageConstant.DELAYED_DEAD_QUEUE, ExchangeConstant.DELAYED_DEAD_LETTER.getExchangeName(), MessageConstant.DELAYED_DEAD_ROUTING);

            // 3.声明普通队列
            Map<String, Object> arguments = new HashMap<>();
            // 3.1.过期时间
            arguments.put("x-message-ttl", 30000);
            // 3.2.正常队列设置死信交换机
            arguments.put("x-dead-letter-exchange", ExchangeConstant.DELAYED_DEAD_LETTER.getExchangeName());
            // 3.3.设置死信routing key
            arguments.put("x-dead-letter-routing-key", MessageConstant.DELAYED_DEAD_ROUTING);
            // 3.4.设置正常队列长度限制
            arguments.put("x-max-length", 10);

            channel.queueDeclare(MessageConstant.DELAYED_PRIME_QUEUE, false, false, false, arguments);
            channel.queueBind(MessageConstant.DELAYED_PRIME_QUEUE, ExchangeConstant.DELAYED.getExchangeName()
                    , MessageConstant.DELAYED_PRIME_ROUTING);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                // 不做任何处理，等待消息TTL时间或队列的过期时间后，自动转入到死信队列实现延时效果
//                System.out.println(LocalDateTime.now());
                channel.basicReject(delivery.getEnvelope().getDeliveryTag(), true);
            };

            // 死信生效的条件:设置autoAck=false，且在deliverCallback调用拒绝basicReject
            channel.basicConsume(MessageConstant.DELAYED_PRIME_QUEUE, false, deliverCallback, consumerTag -> {
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
