package com.mashibing.mq;

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
public class ConsumerListener {

    @Autowired
    private RabbitMQConfig mqConfig;

    @Autowired
    private RabbitTemplate rabbitTemplate;


    // 痛点，队列无法通过配置注入
    @RabbitListener(queues = "boot-queue")
    public void consume(String message, Channel channel, Message messageObj) throws IOException {
        // messageObj可以获取到所有信息，message只是为了操作方便
        log.info("接收到的消息：" + message);
        final MessageProperties props = messageObj.getMessageProperties();
        final String uuid = props.getCorrelationId();
        log.info("唯一标识为：" + uuid);
        channel.basicAck(props.getDeliveryTag(), false);
    }
}
