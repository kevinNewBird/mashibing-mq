package com.mashibing.mq.demo.pubsub;

import com.mashibing.mq.constant.ExchangeConstant;
import com.mashibing.mq.constant.MessageConstant;
import com.mashibing.mq.util.RabbitMQConnectUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * description  Publisher <BR>
 * <p>
 * author: zhao.song
 * date: created in 11:40  2022/3/7
 * company: TRS信息技术有限公司
 * version 1.0
 */
@Slf4j
public class PublisherOfPubSub {

    /**
     * description: 广播数据（历史数据不关注，所以未指定队列。由客户端连接时，创建的临时队列接收信息，接收完临时队列被回收）
     * create by: zhaosong 2024/11/5 11:57
     */
    @Test
    public void publish() {
        try (Connection conn = RabbitMQConnectUtil.buildConnection();
             // 1.构建通道channel
             Channel channel = conn.createChannel()
        ) {
            // 2.构建交换机
            channel.exchangeDeclare(ExchangeConstant.PUBSUB.getExchangeName()
                    , ExchangeConstant.PUBSUB.getExchangeType());

            // 3.发送消息
            channel.basicPublish(ExchangeConstant.PUBSUB.getExchangeName(),
                    "", null, "Hello world!".getBytes());
        } catch (Exception ex) {
            System.err.println(String.format("通讯方式【%s】: 发送消息失败！", "pub/sub"));
            ex.printStackTrace();
        }
    }

    /**
     * description: 广播数据（保留历史数据，广播到指定队列）
     * create by: zhaosong 2024/11/5 11:57
     */
    @Test
    public void publishOld() {
        try (
                // 1.获取连接对象
                Connection conn = RabbitMQConnectUtil.buildConnection();
                // 2.构建channel
                Channel channel = conn.createChannel()) {

            // 3.构建交换机
            channel.exchangeDeclare(ExchangeConstant.PUBSUB.getExchangeName()
                    , ExchangeConstant.PUBSUB.getExchangeType());
            // 4.构建队列
            /** 参数说明：
             *  durable: 是否持久化队列。true表示持久化队列，服务器重启后也可使用该队列
             *  exclusive: 是否允许多个消费者使用。true表示排外的，只允许一个消费者使用该队列，否则会报错
             *  autoDelete: 是否自动删除。true表示如果检测到该队列长时间没有被使用，服务器将自动删除它
             *  arguments: 其它参数设置
             */
            channel.queueDeclare(MessageConstant.PUB_SUB_QUEUE_NAME1, false, false, false, null);
            channel.queueDeclare(MessageConstant.PUB_SUB_QUEUE_NAME2, false, false, false, null);
            // 5.绑定交换机和队列，使用的FANOUT类型的交换机，绑定方式是直接绑定，也就是说routingKey写什么都是允许的
            channel.queueBind(MessageConstant.PUB_SUB_QUEUE_NAME1
                    , ExchangeConstant.PUBSUB.getExchangeName(), "");
            channel.queueBind(MessageConstant.PUB_SUB_QUEUE_NAME2
                    , ExchangeConstant.PUBSUB.getExchangeName(), "");

            // 6.发布消息到交换机(fanout类型的交换机会忽略routingKey,所以无需传入)
            channel.basicPublish(ExchangeConstant.PUBSUB.getExchangeName(), "", null, "Hello world!".getBytes());

            // 这段代码的作用是为了查看图形界面的connections和channels
//            System.in.read();
        } catch (IOException | TimeoutException e) {
            System.err.println(String.format("通讯方式【%s】: 发送消息失败！", "pub/sub"));
            e.printStackTrace();
        }
    }
}
