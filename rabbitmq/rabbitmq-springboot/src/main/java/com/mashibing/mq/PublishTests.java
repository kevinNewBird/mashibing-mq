package com.mashibing.mq;

import com.mashibing.mq.conifg.RabbitMQConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;

/**
 * description  PublishTest <BR>
 * <p>
 * author: zhao.song
 * date: created in 13:25  2022/3/8
 * company: TRS信息技术有限公司
 * version 1.0
 */
@SpringBootTest
@Slf4j
public class PublishTests {

    @Autowired
    private RabbitMQConfig mqConfig;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    // 简单的消息发送
    @Test
    public void simplePublish() {
        rabbitTemplate.convertAndSend(mqConfig.getExchange(), "big.black.dog", "***message***");
        log.info("消息发送成功！");
    }

    // 简单消息发送携带一些信息
    @Test
    public void simplePublishWithExtraMessage() {
        rabbitTemplate.convertAndSend(mqConfig.getExchange(), "big.black.dog"
                , "***message props***", new MessagePostProcessor() {
                    @Override
                    public Message postProcessMessage(Message message) throws AmqpException {
                        final MessageProperties props = message.getMessageProperties();
                        props.setCorrelationId(UUID.randomUUID().toString());
                        return message;
                    }
                });
        log.info("消息发送成功！");
    }
}
