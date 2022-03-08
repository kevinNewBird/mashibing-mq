package com.mashibing.mq.util;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * description  原生java api<BR>
 * <p>
 * author: zhao.song
 * date: created in 11:22  2022/3/7
 * company: TRS信息技术有限公司
 * version 1.0
 */
public final class RabbitMQConnectUtil {

    private static final String RABBITMQ_HOST = "101.132.151.4";
    private static final int RABBITMQ_PORT = 5672;
    private static final String RABBITMQ_VIRTUALHOST = "/";
    private static final String RABBITMQ_USERNAME = "guest";
    private static final String RABBITMQ_PASSWORD = "guest";

    /**
     * description   建立rabbitmq连接对象  <BR>
     *     <br/>
     *     <br/>
     *     文档参考 https://www.rabbitmq.com/api-guide.html#connecting
     *     连接方式有多种，请根据需要自行在官网进行选择
     *
      * @param :
     * @return {@link Connection}
     * @author zhao.song  2022/3/7  11:23
     */
    public static Connection buildConnection() throws IOException, TimeoutException {
        // 1.创建connection工厂
        ConnectionFactory cf = new ConnectionFactory();
        // 2.设置连接信息
        cf.setHost(RABBITMQ_HOST);
        cf.setPort(RABBITMQ_PORT);
        cf.setUsername(RABBITMQ_USERNAME);
        cf.setPassword(RABBITMQ_PASSWORD);
        cf.setVirtualHost(RABBITMQ_VIRTUALHOST);// 默认为“/”
        // 3.返回连接对象
        return cf.newConnection();
    }
}
