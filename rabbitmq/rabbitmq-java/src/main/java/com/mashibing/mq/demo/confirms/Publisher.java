package com.mashibing.mq.demo.confirms;

import com.mashibing.mq.constant.MessageConstant;
import com.mashibing.mq.util.RabbitMQConnectUtil;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * description  Publisher <BR>
 * <p>
 * author: zhao.song
 * date: created in 15:07  2022/3/8
 * company: TRS信息技术有限公司
 * version 1.0
 */
@Slf4j
public class Publisher {


    @Test
    public void publish() {
        try (
                // 1.获取连接对象
                Connection conn = RabbitMQConnectUtil.buildConnection();
                // 2.构建channel
                Channel channel = conn.createChannel()) {

            // 3.构建队列
            channel.queueDeclare(MessageConstant.CONFIRMS_QUEUE_CONSUMER, true, false, false, null);

            // 4.开启confirms
            channel.confirmSelect();

            // 5.设置 confirms的异步回调
            channel.addConfirmListener(new ConfirmListener() {
                @Override
                public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                    System.out.println("消息成功的发送到Exchange");
                }

                @Override
                public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                    System.out.println("消息没有发送到Exchange，尝试重试或者持久化做补偿操作！");
                }
            });
            // 6.开启return机制,必须在消息发送时携带一个参数mandatory=true
            channel.addReturnListener(new ReturnListener() {
                @Override
                public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    System.out.println("消息没有送达到Queue队列中，做其它的补偿措施！");
                }
            });

            // 7.设置消息持久化(参考MessageProperties)
            final AMQP.BasicProperties props = new AMQP.BasicProperties().builder()
                    .deliveryMode(2)
                    .build();

            // 8.发布消息(默认交换机就是空串),但是routingKey是必须要有的
            channel.basicPublish("", MessageConstant.CONFIRMS_QUEUE_CONSUMER,true
                    , props, "Hello confirms!".getBytes());

            // 这段代码的作用是为了查看图形界面的connections和channels
            System.in.read();
        } catch (IOException | TimeoutException e) {
            System.err.println(String.format("通讯方式【%s】: 发送消息失败！", "confirms"));
            e.printStackTrace();
        }
    }
}
