package com.mashibing.mq.demo.routing;

import com.mashibing.mq.constant.ExchangeConstant;
import com.mashibing.mq.constant.MessageConstant;
import com.mashibing.mq.util.RabbitMQConnectUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Envelope;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static com.mashibing.mq.constant.ExchangeConstant.ROUTING;

/**
 * description  Consumer <BR>
 * <p>
 * author: zhao.song
 * date: created in 17:13  2022/3/7
 * company: TRS信息技术有限公司
 * version 1.0
 */
@Slf4j
public class ConsumerTwoOfRouting {

    /**
     * 对于临时队列，rk2和rk3都绑定在在了routing2队列上，那么消费的时候将会严格执行绑定键，即消费者只绑定了rk2那么只会接收到rk2的消息
     * 且历史信息不会被消费
     * description:
     * create by: zhaosong 2024/11/5 14:57
     */
    @Test
    public void consume() {
        try (Connection conn = RabbitMQConnectUtil.buildConnection();
             Channel channel = conn.createChannel()) {
            // 1.创建交换机
            channel.exchangeDeclare(ROUTING.getExchangeName(), ROUTING.getExchangeType());
            // 2.创建临时队列并绑定
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, ROUTING.getExchangeName(), MessageConstant.ROUTING_KEY2);
            // 3.回调
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message;
                try {
                    message = new String(delivery.getBody(), "utf-8");
                    final Envelope envelope = delivery.getEnvelope();
                    // 消息
                    System.out.println("[x] [routing] received '" + message + "'");
                } finally {
                    // 消息处理完后，手动响应
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            };
            // 4.接收消息（关闭自动响应autoAck=false）
            channel.basicConsume(queueName, false, deliverCallback, consumerTag -> {
            });
            // 阻塞，保证线程可以消费到
            System.in.read();
        } catch (IOException | TimeoutException e) {
            System.err.println(String.format("通讯方式【%s】: 接收消息失败！", "routing"));
            e.printStackTrace();
        }
    }

    /**
     * 对于保留队列，rk2和rk3都绑定在在了routing2队列上，那么消费的时候将会忽略绑定键，即尽管消费者只绑定了rk2但rk2和rk3的消息都会被消费
     * description:
     * create by: zhaosong 2024/11/5 14:55
     */
    @Test
    public void consumeOld() {
        try (final Connection conn = RabbitMQConnectUtil.buildConnection();
             final Channel channel = conn.createChannel()) {
            // 1.指定交换机
            channel.exchangeDeclare(ROUTING.getExchangeName()
                    , ROUTING.getExchangeType());
            channel.basicQos(1);
            // 2.获取分配队列的名字，并绑定(只会接受队列routing2的且绑定键为rk2和rk3的消息)
            // 注意：消费端，就算队列的绑定键只指定了一个，另一个rk3的消息也会被接收到（换言之，以routing key的分组来决定消息的获取）
            channel.queueBind(MessageConstant.ROUTING_QUEUE_NAME2
                    , ROUTING.getExchangeName(), MessageConstant.ROUTING_KEY2);
            channel.queueBind(MessageConstant.ROUTING_QUEUE_NAME2
                    , ROUTING.getExchangeName(), MessageConstant.ROUTING_KEY3);
            // 3.
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "utf-8");
                final Envelope envelope = delivery.getEnvelope();
                channel.basicAck(envelope.getDeliveryTag(), false);
                System.out.println("[routing] received '" + message + "'");
            };
            // 4.接收消息
            channel.basicConsume(MessageConstant.ROUTING_QUEUE_NAME2, deliverCallback, consumerTag -> {
            });
            // 阻塞，保证线程可以消费到
            System.in.read();
        } catch (IOException | TimeoutException e) {
            System.err.println((String.format("通讯方式【%s】: 接收消息失败！", "routing")));
            e.printStackTrace();
        }
    }
}
