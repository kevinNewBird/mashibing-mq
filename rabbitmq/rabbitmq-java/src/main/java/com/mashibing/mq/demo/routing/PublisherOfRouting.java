package com.mashibing.mq.demo.routing;

import com.mashibing.mq.constant.ExchangeConstant;
import com.mashibing.mq.constant.MessageConstant;
import com.mashibing.mq.util.RabbitMQConnectUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static com.mashibing.mq.constant.ExchangeConstant.ROUTING;

/**
 * description  Publisher <BR>
 * <p>
 * author: zhao.song
 * date: created in 11:40  2022/3/7
 * company: TRS信息技术有限公司
 * version 1.0
 */
@Slf4j
public class PublisherOfRouting {

    /**
     * 本质上是direct交换机，是对fanout交换机的一种场景的扩展
     * description:
     * create by: zhaosong 2024/11/5 15:07
     */
    @Test
    public void publish() {
        try (
                // 1.获取连接对象
                Connection conn = RabbitMQConnectUtil.buildConnection();
                // 2.构建channel
                Channel channel = conn.createChannel()) {
            // 3.创建交换机
            channel.exchangeDeclare(ROUTING.getExchangeName(), ROUTING.getExchangeType());

            // 4.发送消息（发送到指定的绑定键[routing key]）
            channel.basicPublish(ROUTING.getExchangeName(), MessageConstant.ROUTING_KEY1, null, "[rk1] Hello world!".getBytes());
            channel.basicPublish(ROUTING.getExchangeName(), MessageConstant.ROUTING_KEY2, null, "[rk2] Hello world!".getBytes());
            channel.basicPublish(ROUTING.getExchangeName(), MessageConstant.ROUTING_KEY3, null, "[rk3] Hello world!".getBytes());
        } catch (IOException | TimeoutException e) {
            System.err.println(String.format("通讯方式【%s】: 发送消息失败！", "routing"));
            e.printStackTrace();
        }
    }


    /**
     * description: 如果说发布订阅（fanout交换机）是将数据无状态的发送到所有队列上，那么routing交换机只将消息发送到和routing key绑定的队列上
     * <TIP/> 注意：绑定指定保留队列（当然使用的时候可以使用临时队列和交换机建立新的绑定关系）
     * <br/>
     * 使用相同的绑定键(routing key)绑定多个队列是完全合法的。在这样的设置中，使用路由键发布到交换器的消息 orange将被路由到队列Q1。
     * 路由键为black 或 的消息green将发送到Q2。所有其他消息将被丢弃。
     * create by: zhaosong 2024/11/5 14:10
     */
    @Test
    public void publishOld() {
        try (
                // 1.获取连接对象
                Connection conn = RabbitMQConnectUtil.buildConnection();
                // 2.构建channel
                Channel channel = conn.createChannel()) {

            // 3.构建交换机
            channel.exchangeDeclare(ROUTING.getExchangeName(), ROUTING.getExchangeType());

            // 4.构建队列
            channel.queueDeclare(MessageConstant.ROUTING_QUEUE_NAME1, false, false, false, null);
            channel.queueDeclare(MessageConstant.ROUTING_QUEUE_NAME2, false, false, false, null);

            // 5.绑定队列
            channel.queueBind(MessageConstant.ROUTING_QUEUE_NAME1, ROUTING.getExchangeName(), MessageConstant.ROUTING_KEY1);
            // 一个队列绑定多个consumer,只需要改变下routingKey即可
            channel.queueBind(MessageConstant.ROUTING_QUEUE_NAME2, ROUTING.getExchangeName(), MessageConstant.ROUTING_KEY2);
            channel.queueBind(MessageConstant.ROUTING_QUEUE_NAME2, ROUTING.getExchangeName(), MessageConstant.ROUTING_KEY3);

            // 6.发送消息
            channel.basicPublish(ROUTING.getExchangeName(), MessageConstant.ROUTING_KEY1, null, "[rk1] Hello world!".getBytes());
            channel.basicPublish(ROUTING.getExchangeName(), MessageConstant.ROUTING_KEY2, null, "[rk2] Hello world!".getBytes());
            channel.basicPublish(ROUTING.getExchangeName(), MessageConstant.ROUTING_KEY3, null, "[rk3] Hello world!".getBytes());
        } catch (IOException | TimeoutException e) {
            System.err.println(String.format("通讯方式【%s】: 发送消息失败！", "routing"));
            e.printStackTrace();
        }
    }
}
