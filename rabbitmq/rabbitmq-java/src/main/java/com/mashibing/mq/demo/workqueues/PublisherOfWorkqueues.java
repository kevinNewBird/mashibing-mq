package com.mashibing.mq.demo.workqueues;

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
public class PublisherOfWorkqueues {


    @Test
    public void publish() throws IOException {
        try (
                // 1.获取连接对象
                Connection conn = RabbitMQConnectUtil.buildConnection();
                // 2.构建channel
                Channel channel = conn.createChannel()) {

            // 3.构建队列
            /** 参数说明：
             *  durable: 是否持久化队列。true表示持久化队列，服务器重启后也可使用该队列
             *  exclusive: 是否允许多个消费者使用。true表示排外的，只允许一个消费者使用该队列，否则会报错
             *  autoDelete: 是否自动删除。true表示如果检测到该队列长时间没有被使用，服务器将自动删除它
             *  arguments: 其它参数设置
             */
            channel.queueDeclare(MessageConstant.WORK_QUEUES_QUEUE_NAME, false, false, false, null);
            // 4.发布消息(默认交换机就是空串)
            for (int index = 0; index < 10; index++) {
                channel.basicPublish("", MessageConstant.WORK_QUEUES_QUEUE_NAME, null, (index + "Hello world!").getBytes());
            }

            // 这段代码的作用是为了查看图形界面的connections和channels
        } catch (IOException | TimeoutException e) {
            System.err.println(String.format("通讯方式【%s】: 发送消息失败！", "workqueues"));
            e.printStackTrace();
        }


    }
}
