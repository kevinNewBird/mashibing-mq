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


    @Test
    public void publish() {
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
            channel.queueBind(MessageConstant.ROUTING_QUEUE_NAME1, ROUTING.getExchangeName(), "rk1");
            // 一个队列绑定多个consumer,只需要改变下routingKey即可
            channel.queueBind(MessageConstant.ROUTING_QUEUE_NAME2, ROUTING.getExchangeName(), "rk2");
            channel.queueBind(MessageConstant.ROUTING_QUEUE_NAME2, ROUTING.getExchangeName(), "rk3");

            // 6.发送消息
            channel.basicPublish(ROUTING.getExchangeName(),"rk1", null, "[rk1] Hello world!".getBytes());
            channel.basicPublish(ROUTING.getExchangeName(),"rk2", null, "[rk2] Hello world!".getBytes());
            channel.basicPublish(ROUTING.getExchangeName(),"rk3", null, "[rk3] Hello world!".getBytes());
        } catch (IOException | TimeoutException e) {
            System.err.println(String.format("通讯方式【%s】: 发送消息失败！", "routing"));
            e.printStackTrace();
        }
    }
}
