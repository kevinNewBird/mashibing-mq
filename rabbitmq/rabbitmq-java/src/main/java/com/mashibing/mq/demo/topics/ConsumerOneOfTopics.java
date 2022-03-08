package com.mashibing.mq.demo.topics;

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
import static com.mashibing.mq.constant.ExchangeConstant.TOPICS;

/**
 * description  Consumer <BR>
 * <p>
 * author: zhao.song
 * date: created in 17:13  2022/3/7
 * company: TRS信息技术有限公司
 * version 1.0
 */
@Slf4j
public class ConsumerOneOfTopics {

    @Test
    public void consume() {
        try (final Connection conn = RabbitMQConnectUtil.buildConnection();
             final Channel channel = conn.createChannel()) {
            // 1.指定交换机
            channel.exchangeDeclare(TOPICS.getExchangeName()
                    ,TOPICS.getExchangeType());
            channel.basicQos(1);
            // 2.获取分配队列的名字，并绑定
            final String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName
                    , TOPICS.getExchangeName(), "*.orange.*");
            // 3.
            DeliverCallback deliverCallback = (consumerTag,delivery)->{
                String message = new String(delivery.getBody(), "utf-8");
                final Envelope envelope = delivery.getEnvelope();
                channel.basicAck(envelope.getDeliveryTag(), false);
                System.out.println("[topics] received '" + message + "'");
            };
            // 4.接收消息
            channel.basicConsume(MessageConstant.TOPICS_QUEUE_NAME1, deliverCallback, consumerTag -> {});
            // 阻塞，保证线程可以消费到
            System.in.read();
        } catch (IOException | TimeoutException e) {
            log.error(String.format("通讯方式【%s】: 接收消息失败！", "topics"), e);
        }
    }
}
