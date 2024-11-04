package com.mashibing.mq.demo.pubsub;

import com.mashibing.mq.constant.ExchangeConstant;
import com.mashibing.mq.constant.MessageConstant;
import com.mashibing.mq.util.RabbitMQConnectUtil;
import com.rabbitmq.client.*;
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
public class ConsumerOneOfPubSub {

    @Test
    public void consume() {
        try (final Connection conn = RabbitMQConnectUtil.buildConnection();
             final Channel channel = conn.createChannel()) {
            // 1.构建交换机
            channel.exchangeDeclare(ExchangeConstant.PUBSUB.getExchangeName()
                    , ExchangeConstant.PUBSUB.getExchangeType());
            channel.basicQos(1);
            // 2.获取分配队列的名字，并绑定
            final String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName
                    , ExchangeConstant.PUBSUB.getExchangeName(), "");
            // 3.
            DeliverCallback deliverCallback = (consumerTag,delivery)->{
                String message = new String(delivery.getBody(), "utf-8");
                final Envelope envelope = delivery.getEnvelope();
                channel.basicAck(envelope.getDeliveryTag(), false);
                System.out.println("[pub/sub] received '" + message + "'");
            };
            // 4.接收消息
            channel.basicConsume(MessageConstant.PUB_SUB_QUEUE_NAME1, deliverCallback, consumerTag -> {});
            // 阻塞，保证线程可以消费到
            System.in.read();
        } catch (IOException | TimeoutException e) {
            log.error(String.format("通讯方式【%s】: 接收消息失败！", "pub/sub"), e);
            e.printStackTrace();
        }
    }
}
