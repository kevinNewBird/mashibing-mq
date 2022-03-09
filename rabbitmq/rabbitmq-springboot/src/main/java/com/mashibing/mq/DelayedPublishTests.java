package com.mashibing.mq;

import com.mashibing.mq.conifg.DelayedExchangeConfig;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * description  DelayedPublishTests <BR>
 * <p>
 * author: zhao.song
 * date: created in 14:15  2022/3/9
 * company: TRS信息技术有限公司
 * version 1.0
 */
@SpringBootTest
public class DelayedPublishTests {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Test
    public void publish() {
        rabbitTemplate.convertAndSend(DelayedExchangeConfig.DELAYED_EXCHANGE
                , "delayed.abc", "delayed msg", new MessagePostProcessor() {
                    @Override
                    public Message postProcessMessage(Message message) throws AmqpException {
                        final MessageProperties props = message.getMessageProperties();
                        // 发送消息30秒后，消息才会进入到延迟队列中
                        props.setDelay(30000);
                        return message;
                    }
                });
    }
}
