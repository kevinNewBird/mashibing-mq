package com.mashibing.mq.conifg;

import lombok.Data;
import lombok.Setter;
import org.springframework.amqp.core.*;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


/**
 * description  声明交换机和队列 <BR>
 * <p>
 * author: zhao.song
 * date: created in 13:14  2022/3/8
 * company: TRS信息技术有限公司
 * version 1.0
 */
@Configuration
@ConfigurationProperties(prefix = "rabbitmq")
@Data
public class RabbitMQConfig {

    public String exchange;

    public String queue;

    public String routing;


    @Bean
    public Exchange bootExchange() {
        // 等价于channel.exchangeDeclare
        return ExchangeBuilder.topicExchange(exchange).build();
    }

    @Bean
    public Queue bootQueue() {
        return QueueBuilder.durable(queue).build();
    }

    @Bean
    public Binding bootBindings(Exchange bootExchange, Queue bootQueue) {
        return BindingBuilder.bind(bootQueue).to(bootExchange).with(routing).noargs();
    }
}
