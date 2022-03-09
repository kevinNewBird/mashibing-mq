package com.mashibing.mq.conifg;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * description  延迟交换机配置 <BR>
 * <p>
 * author: zhao.song
 * date: created in 14:08  2022/3/9
 * company: TRS信息技术有限公司
 * version 1.0
 */
@Configuration
public class DelayedExchangeConfig {

    public static final String DELAYED_EXCHANGE = "delayed-exchange";
    public static final String DELAYED_QUEUE = "delayed-queue";
    public static final String DELAYED_ROUTING_KEY = "delayed.#";

    @Bean
    public Exchange delayedExchange() {
        final Map<String, Object> args = new HashMap<>();
        // 告知服务，x-delayed-type的交换机类型为topic
        args.put("x-delayed-type", "topic");
        // 这个类型是我们插件安装成功后
        Exchange exchange = new CustomExchange(DELAYED_EXCHANGE, "x-delayed-message", true, false, args);
        return exchange;
    }

    @Bean
    public Queue delayedQueue() {
        return QueueBuilder.durable(DELAYED_QUEUE).build();
    }

    @Bean
    public Binding delayedBinding(Exchange delayedExchange,Queue delayedQueue) {
        return BindingBuilder.bind(delayedQueue).to(delayedExchange).with(DELAYED_ROUTING_KEY).noargs();
    }
}
