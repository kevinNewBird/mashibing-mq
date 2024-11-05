package com.mashibing.mq.demo.hellword;

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
public class ConsumerOfHelloWorld {

    @Test
    public void consume() {
        try (final Connection conn = RabbitMQConnectUtil.buildConnection();
             final Channel channel = conn.createChannel()) {
            // 1.创建队列
            channel.queueDeclare(MessageConstant.HELLO_WORLD_QUEUE_NAME, false, false, false, null);

            // 2.监听消息
            // 2.1.构建监听消息的回调
            DefaultConsumer consumer = new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    System.out.println("消费者接收到的消息：" + new String(body, "utf-8"));
                }
            };
            // 2.2.开启监听消息
            channel.basicConsume(MessageConstant.HELLO_WORLD_QUEUE_NAME,true, consumer);
            // 阻塞，保证线程可以消费到
            System.in.read();
        } catch (IOException | TimeoutException e) {
            System.err.println(String.format("通讯方式【%s】: 接收消息失败！", "hello world"));
            e.printStackTrace();
        }
    }
}
