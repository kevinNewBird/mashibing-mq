package com.mashibing.mq.demo.rpc;

import com.mashibing.mq.constant.MessageConstant;
import com.mashibing.mq.util.RabbitMQConnectUtil;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static com.mashibing.mq.constant.MessageConstant.RPC_QUEUE_CONSUMER;


/**
 * description  Publisher <BR>
 * <p>
 * author: zhao.song
 * date: created in 10:59  2022/3/8
 * company: TRS信息技术有限公司
 * version 1.0
 */
@Slf4j
public class PublisherOfRPC {

    @Test
    public void publish() {
        try (final Connection conn = RabbitMQConnectUtil.buildConnection();
             final Channel channel = conn.createChannel()) {

            // 1.构建队列
            channel.queueDeclare(MessageConstant.RPC_QUEUE_PUBLISHER, false, false, false, null);
            channel.queueDeclare(MessageConstant.RPC_QUEUE_CONSUMER, false, false, false, null);

            // 2.发送消息
            final String uuid = UUID.randomUUID().toString();
            AMQP.BasicProperties props = new AMQP.BasicProperties().builder()
                    // 监听的队列(也就是说我们的消息发送到RPC_QUEUE_PUBLISHER队列，同时监听RPC_QUEUE_CONSUMER队列)
                    .replyTo(MessageConstant.RPC_QUEUE_CONSUMER)
                    // 唯一标识
                    .correlationId(uuid)
                    .build();
            channel.basicPublish("",MessageConstant.RPC_QUEUE_PUBLISHER, props, "hello rpc！".getBytes());

            channel.basicConsume(RPC_QUEUE_CONSUMER, false, new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    final String id = properties.getCorrelationId();
                    if (id != null && id.equals(uuid)) {
                        System.out.println("消费者One接收到的消息：" + new String(body, "utf-8"));
                        channel.basicAck(envelope.getDeliveryTag(), false);
                    }
                }
            });
            System.in.read();
        } catch (IOException | TimeoutException e) {
            System.err.println(String.format("通讯方式【%s】: 接收消息失败！", "rpc"));
            e.printStackTrace();
        }
    }

}
