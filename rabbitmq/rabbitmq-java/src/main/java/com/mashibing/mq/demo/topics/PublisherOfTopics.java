package com.mashibing.mq.demo.topics;

import com.mashibing.mq.constant.MessageConstant;
import com.mashibing.mq.util.RabbitMQConnectUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static com.mashibing.mq.constant.ExchangeConstant.ROUTING;
import static com.mashibing.mq.constant.ExchangeConstant.TOPICS;

/**
 * description  Publisher <BR>
 * <p>
 * author: zhao.song
 * date: created in 11:40  2022/3/7
 * company: TRS信息技术有限公司
 * version 1.0
 */
@Slf4j
public class PublisherOfTopics {


    @Test
    public void publish() {
        try (
                // 1.获取连接对象
                Connection conn = RabbitMQConnectUtil.buildConnection();
                // 2.构建channel
                Channel channel = conn.createChannel()) {

            // 3.构建交换机
            channel.exchangeDeclare(TOPICS.getExchangeName(), TOPICS.getExchangeType());

            // 4.构建队列
            channel.queueDeclare(MessageConstant.TOPICS_QUEUE_NAME1, false, false, false, null);
            channel.queueDeclare(MessageConstant.TOPICS_QUEUE_NAME2, false, false, false, null);

            // 5.绑定队列
            // TOPIC类型的交换机和队列绑定时，需要以aaa.bbb.ccc方式编写routingKey
            // 其中有两个特殊字符：*（相当于占位符），#（相当于通配符）
            // aaa.orange.bbb: 路由到TOPICS_QUEUE_NAME1队列
            // aaa.orange.rabbit: 路由到TOPICS_QUEUE_NAME1 和 TOPICS_QUEUE_NAME2 队列
            // lazy.orange.rabbit....: 路由到 TOPICS_QUEUE_NAME2 队列
            channel.queueBind(MessageConstant.TOPICS_QUEUE_NAME1, TOPICS.getExchangeName(), "*.orange.*");
            channel.queueBind(MessageConstant.TOPICS_QUEUE_NAME2, TOPICS.getExchangeName(), "*.*.rabbit");
            channel.queueBind(MessageConstant.TOPICS_QUEUE_NAME2, TOPICS.getExchangeName(), "lazy.#");

            // 6.发送消息
            channel.basicPublish(TOPICS.getExchangeName(),"big.orange.rabbit", null, "大橙兔子！".getBytes());
            channel.basicPublish(TOPICS.getExchangeName(),"small.white.rabbit", null, "小白兔！".getBytes());
            channel.basicPublish(TOPICS.getExchangeName(),"lazy.dog.dog.dog", null, "懒狗狗狗狗狗狗狗".getBytes());
        } catch (IOException | TimeoutException e) {
            System.err.println(String.format("通讯方式【%s】: 发送消息失败！", "topics"));
            e.printStackTrace();
        }
    }
}
