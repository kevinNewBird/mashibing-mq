package com.mashibing.mq.constant;

import com.rabbitmq.client.BuiltinExchangeType;
import lombok.Getter;

/**
 * description  ExchangeConstant <BR>
 * <p>
 * author: zhao.song
 * date: created in 23:44  2022/3/7
 * company: TRS信息技术有限公司
 * version 1.0
 */
public enum ExchangeConstant {

    PUBSUB("pubsub", BuiltinExchangeType.FANOUT.getType()),

    ROUTING("routing", BuiltinExchangeType.DIRECT.getType()),


    TOPICS("topics", BuiltinExchangeType.TOPIC.getType()),

    DEAD_GENERAL("dead-general-exchange", BuiltinExchangeType.DIRECT.getType()),

    DEAD_LETTER("dead-exchange", BuiltinExchangeType.DIRECT.getType()),

    DELAYED("delayed-exchange", BuiltinExchangeType.DIRECT.getType()),

    DELAYED_DEAD_LETTER("delayed-dead-exchange", BuiltinExchangeType.DIRECT.getType()),

    THRD_DELAYED("third-delayed-exchange","x-delayed-message"),


    ;

    @Getter
    private String exchangeName;

    @Getter
    private String exchangeType;

    ExchangeConstant(String exchangeName, String exchangeType) {
        this.exchangeName = exchangeName;
        this.exchangeType = exchangeType;
    }
}
