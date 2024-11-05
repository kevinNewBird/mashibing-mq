package com.mashibing.mq.demo.delayed_plugin;

import com.mashibing.mq.constant.ExchangeConstant;
import com.mashibing.mq.util.RabbitMQConnectUtil;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * description：TODO
 *
 * @author zhaosong
 * @version 1.0
 * @company 北京海量数据有限公司
 * @date 2024/11/5 23:33
 */
public class DelayedPublisher {


    public static final String QUEUE_NAME = "third_delayed_queue";

    public static final String ROUTING_KEY = "third_delayed_routing";

    @Test
    public void publish() {
        try (Connection conn = RabbitMQConnectUtil.buildConnection();
             Channel channel = conn.createChannel()) {

            // 2.创建交换机
            Map<String, Object> arguments = new HashMap<>();
            // 2.1.交换机类型
            arguments.put("x-delayed-type", "direct");
            channel.exchangeDeclare(ExchangeConstant.THRD_DELAYED.getExchangeName(), ExchangeConstant.THRD_DELAYED.getExchangeType()
                    , true, false, arguments);

            // 创建一个延时队列
//            Map<String, Object> queueArgs = new HashMap<>();
//            queueArgs.put("x-dead-letter-exchange", "");
//            queueArgs.put("x-dead-letter-routing-key", ROUTING_KEY);
//            queueArgs.put("x-message-ttl", 30000);
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channel.queueBind(QUEUE_NAME, ExchangeConstant.THRD_DELAYED.getExchangeName(), ROUTING_KEY);

            // 发送消息到延时队列中
            Map<String, Object> headers = new HashMap<>();
            headers.put("x-delay", 5000);
            AMQP.BasicProperties props = new AMQP.BasicProperties().builder()
                    .headers(headers)
                    .build();
            channel.basicPublish(ExchangeConstant.THRD_DELAYED.getExchangeName(), ROUTING_KEY
                    , props, "[third delayed] hello delayed queue".getBytes());
        } catch (Exception ex) {
            System.err.println(String.format("[delayed] 通讯方式【%s】: 发送消息失败！", "direct"));
            ex.printStackTrace();
        }
    }
}
