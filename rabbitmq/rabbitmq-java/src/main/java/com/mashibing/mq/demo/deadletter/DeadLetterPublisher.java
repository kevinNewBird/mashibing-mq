package com.mashibing.mq.demo.deadletter;

import com.mashibing.mq.constant.ExchangeConstant;
import com.mashibing.mq.constant.MessageConstant;
import com.mashibing.mq.util.RabbitMQConnectUtil;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.Test;

/**
 * description: 普通交换机（生产死信信息）
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/11/5
 * version: 1.0
 */
public class DeadLetterPublisher {


    /**
     * 死信的来源：
     * 1.消息 TTL 过期
     * 2.队列满了，无法再次添加数据
     * 3.消息被拒绝（reject 或 nack），并且 requeue =false
     * description:
     * create by: zhaosong 2024/11/5 18:07
     */
    @Test
    public void publish() {
        // 1.创建连接和通道
        try (Connection conn = RabbitMQConnectUtil.buildConnection();
             Channel channel = conn.createChannel()) {
            // 2.创建交互机（direct）
            channel.exchangeDeclare(ExchangeConstant.DEAD_GENERAL.getExchangeName()
                    , ExchangeConstant.DEAD_GENERAL.getExchangeType());

            // 3.创建队列
            channel.queueDeclare(MessageConstant.DEAD_GENERAL_QUEUE, false, false, false, null);
            channel.queueBind(MessageConstant.DEAD_GENERAL_QUEUE, ExchangeConstant.DEAD_GENERAL.getExchangeName()
                    , MessageConstant.DEAD_GENERAL_ROUTING);

            // 设置过期时间
            AMQP.BasicProperties props = new AMQP.BasicProperties().builder()
                    .expiration("1000")
                    .build();

            // 4.发送消息
            for (int i = 0; i < 10; i++) {
                channel.basicPublish(ExchangeConstant.DEAD_GENERAL.getExchangeName(), MessageConstant.DEAD_GENERAL_ROUTING
                        , props, String.format("[%s] dead letter test", i).getBytes());
            }
        } catch (Exception ex) {
            System.err.println(String.format("[dead letter] 通讯方式【%s】: 发送消息失败！", "routing"));
            ex.printStackTrace();
        }
    }
}