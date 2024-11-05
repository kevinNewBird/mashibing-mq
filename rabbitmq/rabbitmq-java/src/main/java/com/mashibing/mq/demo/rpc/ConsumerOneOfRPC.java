package com.mashibing.mq.demo.rpc;

import com.mashibing.mq.constant.MessageConstant;
import com.mashibing.mq.util.RabbitMQConnectUtil;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static com.mashibing.mq.constant.ExchangeConstant.TOPICS;
import static com.mashibing.mq.constant.MessageConstant.RPC_QUEUE_CONSUMER;

/**
 * description  Consumer <BR>
 * <p>
 * author: zhao.song
 * date: created in 17:13  2022/3/7
 * company: TRS信息技术有限公司
 * version 1.0
 */
@Slf4j
public class ConsumerOneOfRPC {

    @Test
    public void consume() {
        try (final Connection conn = RabbitMQConnectUtil.buildConnection();
             final Channel channel = conn.createChannel()) {


            channel.basicConsume(MessageConstant.RPC_QUEUE_PUBLISHER, false, new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    System.out.println("消费者One接收到的消息：" + new String(body, "utf-8"));
                    String resp = "获取到了Client发出的请求，这里是响应的信息";
                    final String respQueueName = properties.getReplyTo();
                    final String uuid = properties.getCorrelationId();
                    AMQP.BasicProperties props = new AMQP.BasicProperties().builder()
                            // 唯一标识
                            .correlationId(uuid)
                            .build();
                    channel.basicPublish("",respQueueName,props,resp.getBytes());
                    channel.basicAck(envelope.getDeliveryTag(),false);
                }
            });
            System.in.read();
        } catch (IOException | TimeoutException e) {
            System.err.println(String.format("通讯方式【%s】: 接收消息失败！", "rpc"));
            e.printStackTrace();
        }
    }
}
