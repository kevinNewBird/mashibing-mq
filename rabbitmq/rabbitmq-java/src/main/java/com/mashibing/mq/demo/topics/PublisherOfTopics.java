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

    /**
     * 临时队列
     * description:本质上是topic交换机，是对direct交换机的一种场景的扩展(routing)
     * 尽管使用direct交易所改进了我们的系统，但它仍然存在局限性——它不能根据多种标准进行路由
     * 路由键中可以有任意数量的单词，最多为 255 个字节。它必须是单词列表，以点分隔。
     * <br/>
     * 特殊符号的含义：
     * *(星号) 可以替代一个单词。
     * #(哈希) 可以替代零个或多个单词。
     * create by: zhaosong 2024/11/5 15:08
     */
    @Test
    public void publish() {
        try (
                // 1.获取连接对象
                Connection conn = RabbitMQConnectUtil.buildConnection();
                // 2.构建channel
                Channel channel = conn.createChannel()) {

            // 3.构建交换机
            channel.exchangeDeclare(TOPICS.getExchangeName(), TOPICS.getExchangeType());
            // 4.发送消息
            channel.basicPublish(TOPICS.getExchangeName(), "big.orange.rabbit", null, "大橙兔子！".getBytes());
            channel.basicPublish(TOPICS.getExchangeName(), "small.orange.rabbit", null, "小橙兔子！".getBytes());
            channel.basicPublish(TOPICS.getExchangeName(), "lazy.dog.dog.dog", null, "懒狗狗狗狗狗狗狗".getBytes());
        } catch (IOException | TimeoutException e) {
            System.err.println(String.format("通讯方式【%s】: 发送消息失败！", "topics"));
            e.printStackTrace();
        }
    }

    /**
     * 保留队列
     * create by: zhaosong 2024/11/5 15:17
     */
    @Test
    public void publishOld() {
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
            // lazy.a.b也是被包含的。#表示0或多个单词，单词以句点分割
            channel.queueBind(MessageConstant.TOPICS_QUEUE_NAME2, TOPICS.getExchangeName(), "lazy.#");

            // 6.发送消息
            channel.basicPublish(TOPICS.getExchangeName(), "big.orange.rabbit", null, "大橙兔子！".getBytes());
            channel.basicPublish(TOPICS.getExchangeName(), "small.white.rabbit", null, "小白兔！".getBytes());
            channel.basicPublish(TOPICS.getExchangeName(), "lazy.dog.dog.dog", null, "懒狗狗狗狗狗狗狗".getBytes());
        } catch (IOException | TimeoutException e) {
            System.err.println(String.format("通讯方式【%s】: 发送消息失败！", "topics"));
            e.printStackTrace();
        }
    }
}
