package com.mashibing.mq;

import com.mashibing.mq.conifg.RabbitMQConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CorrelationData;
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



    // 开启confirms机制
    @Test
    public void publishWithConfirms() {
        rabbitTemplate.setConfirmCallback(new RabbitTemplate.ConfirmCallback() {
            @Override
            public void confirm(CorrelationData correlationData, boolean ack, String cause) {
                if (ack) {
                    System.out.println("消息已经送到交换机！！！");
                }else{
                    System.out.println("消息没有送达到Exchange,需要补偿措施！！！retry");
                }
            }
        });
        rabbitTemplate.convertAndSend(mqConfig.getExchange(), "big.black.dog"
                , "***message confirms***", new MessagePostProcessor() {
                    @Override
                    public Message postProcessMessage(Message message) throws AmqpException {
                        final MessageProperties props = message.getMessageProperties();
                        props.setCorrelationId(UUID.randomUUID().toString());
                        return message;
                    }
                });
        log.info("消息发送成功！");
    }

    // 开启return机制
    @Test
    public void publishWithReturn() {
        rabbitTemplate.setReturnsCallback(new RabbitTemplate.ReturnsCallback() {
            @Override
            public void returnedMessage(ReturnedMessage returnedMessage) {
                final String msg = new String(returnedMessage.getMessage().getBody());
                System.out.println("消息:"+msg+"路由到queue队列失败！做补救措施！");
            }
        });

        // 消息时找不到routingKey的，所以一定会进入return机制
        rabbitTemplate.convertAndSend(mqConfig.getExchange(), "big.white.dog"
                , "***message return***", new MessagePostProcessor() {
                    @Override
                    public Message postProcessMessage(Message message) throws AmqpException {
                        final MessageProperties props = message.getMessageProperties();
                        props.setCorrelationId(UUID.randomUUID().toString());
                        return message;
                    }
                });
        log.info("消息发送成功！");
    }


    // 开启持久化
    @Test
    public void publishWithPersistent() {

        // 消息时找不到routingKey的，所以一定会进入return机制
        rabbitTemplate.convertAndSend(mqConfig.getExchange(), "big.black.dog"
                , "***message persistent***", new MessagePostProcessor() {
                    @Override
                    public Message postProcessMessage(Message message) throws AmqpException {
                        final MessageProperties props = message.getMessageProperties();
                        // 持久化设置
                        props.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                        return message;
                    }
                });
        log.info("消息发送成功！");
    }
}
