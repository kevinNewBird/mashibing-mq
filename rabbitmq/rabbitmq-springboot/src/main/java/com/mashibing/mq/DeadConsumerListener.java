package com.mashibing.mq;

import com.mashibing.mq.conifg.DeadLetterConfig;
import com.mashibing.mq.conifg.RabbitMQConfig;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * description  ConsumerTests <BR>
 * <p>
 * author: zhao.song
 * date: created in 13:33  2022/3/8
 * company: TRS信息技术有限公司
 * version 1.0
 */
@Component
@Slf4j
public class DeadConsumerListener {

    @Autowired
    private RabbitMQConfig mqConfig;

    @Autowired
    private RabbitTemplate rabbitTemplate;


    // 痛点，队列无法通过配置注入
    @RabbitListener(queues = DeadLetterConfig.NORMAL_QUEUE)
    public void consume(String message, Channel channel, Message messageObj) throws IOException {
        // messageObj可以获取到所有信息，message只是为了操作方便
        log.info("接收到的消息：" + message);
        // 发送到死信队列
        channel.basicReject(messageObj.getMessageProperties().getDeliveryTag(),false);
        // 说明，multiple表示是否为批量操作，false表示非批量操作
//        channel.basicNack(messageObj.getMessageProperties().getDeliveryTag(),false,false);
    }
}
