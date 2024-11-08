package com.mashibing.mq.demo.pubsub;

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

/**
 * description  Consumer <BR>
 * <p>
 * author: zhao.song
 * date: created in 17:13  2022/3/7
 * company: TRS信息技术有限公司
 * version 1.0
 */
@Slf4j
public class ConsumerTwoOfPubSub {

    /**
     * description: 广播的特点：只接收最新的消息（不关注历史的数据）
     * create by: zhaosong 2024/11/5 11:55
     */
    @Test
    public void consume() {
        try (final Connection conn = RabbitMQConnectUtil.buildConnection(); final Channel channel = conn.createChannel()) {
            // 1.指定交换机
            channel.exchangeDeclare(ExchangeConstant.PUBSUB.getExchangeName(), ExchangeConstant.PUBSUB.getExchangeType());
            channel.basicQos(1);
            // 2.获取分配队列的名字，并绑定
            final String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, ExchangeConstant.PUBSUB.getExchangeName(), "");
            // 3.
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "utf-8");
                final Envelope envelope = delivery.getEnvelope();
                channel.basicAck(envelope.getDeliveryTag(), false);
                System.out.println("[pub/sub] received '" + message + "'");
            };
            // 4.接收消息
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });
            // 阻塞，保证线程可以消费到
            System.in.read();
        } catch (IOException | TimeoutException e) {
            System.err.println(String.format("通讯方式【%s】: 接收消息失败！", "pub/sub"));
            e.printStackTrace();
        }
    }

    /**
     * description: 广播的特点：只接收最新的消息（关注历史的数据）
     * create by: zhaosong 2024/11/5 11:55
     */
    @Test
    public void consumeOld() {
        try (final Connection conn = RabbitMQConnectUtil.buildConnection(); final Channel channel = conn.createChannel()) {
            // 1.指定交换机
            channel.exchangeDeclare(ExchangeConstant.PUBSUB.getExchangeName(), ExchangeConstant.PUBSUB.getExchangeType());
            channel.basicQos(1);
            // 2.获取分配队列的名字，并绑定
            channel.queueBind(MessageConstant.PUB_SUB_QUEUE_NAME2, ExchangeConstant.PUBSUB.getExchangeName(), "");
            // 3.
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "utf-8");
                final Envelope envelope = delivery.getEnvelope();
                channel.basicAck(envelope.getDeliveryTag(), false);
                System.out.println("[pub/sub] received '" + message + "'");
            };
            // 4.接收消息
            channel.basicConsume(MessageConstant.PUB_SUB_QUEUE_NAME2, deliverCallback, consumerTag -> {
            });
            // 阻塞，保证线程可以消费到
            System.in.read();
        } catch (IOException | TimeoutException e) {
            System.err.println(String.format("通讯方式【%s】: 接收消息失败！", "pub/sub"));
            e.printStackTrace();
        }
    }
}
