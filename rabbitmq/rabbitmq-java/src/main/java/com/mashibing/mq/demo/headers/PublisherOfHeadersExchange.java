package com.mashibing.mq.demo.headers;

import com.mashibing.mq.util.RabbitMQConnectUtil;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * description  HeadersExchangePublisher <BR>
 * <p>
 * author: zhao.song
 * date: created in 16:54  2022/3/9
 * company: TRS信息技术有限公司
 * version 1.0
 */
public class PublisherOfHeadersExchange {


    public static final String HEADER_EXCHANGE = "header_exchange";

    public static final String HEADER_QUEUE = "header_queue";

    @Test
    public void publish() {
        try (
                // 1.获取连接对象
                Connection conn = RabbitMQConnectUtil.buildConnection();
                // 2.构建channel
                Channel channel = conn.createChannel()) {

            channel.exchangeDeclare(HEADER_EXCHANGE, BuiltinExchangeType.HEADERS);
            channel.queueDeclare(HEADER_QUEUE, false, false, false, null);
            Map<String, Object> args = new HashMap<>();
            args.put("x-match", "all");
            args.put("name", "jack");
            args.put("age", "23");
            channel.queueBind(HEADER_QUEUE, HEADER_EXCHANGE, "", args);


            Map<String, Object> headers = new HashMap<>();
            headers.put("name", "jack");
            headers.put("age", "23");
            AMQP.BasicProperties props = new AMQP.BasicProperties().builder()
                    .headers(headers)
                    .build();
            channel.basicPublish(HEADER_EXCHANGE, "", props, "header测试消息".getBytes());
            System.out.println("发送消息成功！");
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }
}
