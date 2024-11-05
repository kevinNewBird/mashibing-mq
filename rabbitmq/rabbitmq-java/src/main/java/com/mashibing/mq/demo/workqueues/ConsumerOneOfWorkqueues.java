package com.mashibing.mq.demo.workqueues;

import com.mashibing.mq.constant.MessageConstant;
import com.mashibing.mq.util.RabbitMQConnectUtil;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
public class ConsumerOneOfWorkqueues {

    @Test
    public void consume() {
        try (final Connection conn = RabbitMQConnectUtil.buildConnection();
             final Channel channel = conn.createChannel()) {
            // 1.创建队列
            channel.queueDeclare(MessageConstant.WORK_QUEUES_QUEUE_NAME, true, false, false, null);
            // 1.2.设置消息的流控（即放弃轮询的方式）
            channel.basicQos(1);// 一次只获取一个消息

            // 2.监听消息
//            doConsumeByDefaultConsumer(channel);
            doConsumeByDeliverCallback(channel);
            // 阻塞，保证线程可以消费到
            System.in.read();
        } catch (IOException | TimeoutException e) {
            System.err.println(String.format("通讯方式【%s】: 接收消息失败！", "workqueues"));
            e.printStackTrace();
        }
    }

    // 这种方式会造成服务器性能的浪费(开启了自动ack)
    @Test
    @Deprecated
    public void consumeOld() {
        try (final Connection conn = RabbitMQConnectUtil.buildConnection();
             final Channel channel = conn.createChannel()) {
            // 1.创建队列
            channel.queueDeclare(MessageConstant.WORK_QUEUES_QUEUE_NAME, true, false, false, null);

            // 2.监听消息
            // 2.1.构建监听消息的回调
            DefaultConsumer consumer = new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    System.out.println("消费者接收到的消息：" + new String(body, "utf-8"));
                }
            };
            // 2.2.开启监听消息
            channel.basicConsume(MessageConstant.WORK_QUEUES_QUEUE_NAME,true, consumer);
            // 阻塞，保证线程可以消费到
            System.in.read();
        } catch (IOException | TimeoutException e) {
            System.err.println(String.format("通讯方式【%s】: 接收消息失败！", "workqueues"));
            e.printStackTrace();
        }
    }

    /**
     * description:采用默认的DefaultConsumer
     * create by: zhaosong 2024/11/5 10:21
     *
     * @param channel
     * @throws IOException
     */
    private void doConsumeByDefaultConsumer(Channel channel) throws IOException {
        // 2.1.构建监听消息的回调
        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    String message = new String(body, "UTF-8");
                    System.out.printf("[x] [%s]消费者接收到的消息：%s%n", envelope.getDeliveryTag(), message);
                } finally {
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };
        // 2.2.开启监听消息
        channel.basicConsume(MessageConstant.WORK_QUEUES_QUEUE_NAME, false, consumer);
    }

    /**
     * description: 采用DeliverCallback
     * create by: zhaosong 2024/11/5 10:23
     *
     * @param channel
     * @throws IOException
     */
    private void doConsumeByDeliverCallback(Channel channel) throws IOException {
        // 2.1.构建监听消息的回调
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.printf("[x] [%s]消费者接收到的消息：%s%n", delivery.getEnvelope().getDeliveryTag(), message);
            } finally {
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };
        // 2.2.开启监听消息
        channel.basicConsume(MessageConstant.WORK_QUEUES_QUEUE_NAME
                , false, deliverCallback, consumerTag -> {
                });
    }

}
