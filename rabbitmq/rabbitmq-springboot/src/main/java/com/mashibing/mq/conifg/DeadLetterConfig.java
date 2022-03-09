package com.mashibing.mq.conifg;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.management.MXBean;


/**
 * description  DeadLetterConfig <BR>
 * <p>
 * author: zhao.song
 * date: created in 9:02  2022/3/9
 * company: TRS信息技术有限公司
 * version 1.0
 */
@Configuration
public class DeadLetterConfig {

    public static final String NORMAL_EXCHANGE = "normal-exchange";
    public static final String NORMAL_QUEUE = "normal-queue";
    public static final String NORMAL_ROUTING_KEY = "normal.#";

    public static final String DEAD_EXCHANGE = "dead-exchange";
    public static final String DEAD_QUEUE = "dead-queue";
    public static final String DEAD_ROUTING_KEY = "dead.#";


    @Bean
    public Exchange normalExchange() {
        return ExchangeBuilder.topicExchange(NORMAL_EXCHANGE).build();
    }

    @Bean
    public Queue normalQueue() {
        return QueueBuilder.durable(NORMAL_QUEUE)
                // 绑定死信交换机
                .deadLetterExchange(DEAD_EXCHANGE)
                // 消息成为死信，修改它的routingKey为dead.abc。否则，在死信队列里接收不到消息
                .deadLetterRoutingKey("dead.abc")
                // 设置队列的消息生存时间
                .ttl(10_000)
                // 消息队列最大长度
//                .maxLength(2)
                .build();
    }

    @Bean
    public Binding normalBindings(Exchange normalExchange,Queue normalQueue) {
        return BindingBuilder.bind(normalQueue).to(normalExchange).with(NORMAL_ROUTING_KEY).noargs();
    }


    @Bean
    public Exchange deadExchange() {
        return ExchangeBuilder.topicExchange(DEAD_EXCHANGE).build();
    }

    @Bean
    public Queue deadQueue() {
        return QueueBuilder.durable(DEAD_QUEUE).build();
    }

    @Bean
    public Binding deadBindings(Exchange deadExchange, Queue deadQueue) {
        return BindingBuilder.bind(deadQueue).to(deadExchange).with(DEAD_ROUTING_KEY).noargs();
    }

}
