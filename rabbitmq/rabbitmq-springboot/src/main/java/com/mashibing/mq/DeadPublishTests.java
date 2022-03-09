package com.mashibing.mq;

import com.mashibing.mq.conifg.DeadLetterConfig;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * description  DeadPublishTests <BR>
 * <p>
 * author: zhao.song
 * date: created in 9:24  2022/3/9
 * company: TRS信息技术有限公司
 * version 1.0
 */
@SpringBootTest
public class DeadPublishTests {

    @Autowired
    private RabbitTemplate rabbitTemplate;


    @Test
    public void publish() {
        rabbitTemplate.convertAndSend(DeadLetterConfig.NORMAL_EXCHANGE, "normal.abc", "dead letter");
    }

    @Test
    public void publishExpire() {
        rabbitTemplate.convertAndSend(DeadLetterConfig.NORMAL_EXCHANGE, "normal.abc", "dead letter", new MessagePostProcessor() {
            @Override
            public Message postProcessMessage(Message message) throws AmqpException {
                final MessageProperties props = message.getMessageProperties();
                // 消息5秒未被消费，将过期从而进入死信队列
                props.setExpiration("5000");
                return message;
            }
        });
    }
}
