package com.mashibing.mq.demo.confirms;

import com.mashibing.mq.constant.MessageConstant;
import com.mashibing.mq.util.RabbitMQConnectUtil;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeoutException;

/**
 * description  Publisher <BR>
 * <p>
 * author: zhao.song
 * date: created in 15:07  2022/3/8
 * company: TRS信息技术有限公司
 * version 1.0
 */
@Slf4j
public class Publisher {


    /**
     * 消息可靠性的几个点：
     * 1、保证消息一定送达到Exchange：开启confirm机制
     * 2、保证Exchange上的消息可以路由到Queue：开启return机制并在发送消息时开启mandatory
     * 3、保证Queue可以持久化消息：队列持久化和消息持久化
     * 4、保证消费者可以正常消费消息：消费者关闭basicAck，并手动确认
     * description:
     * create by: zhaosong 2024/11/5 16:56
     */
    @Test
    public void publish() {
        /**
         * 由于监听器的回调方方handleNack的参数是deliverTag，这个是在当前channel的递增的。而我们发送的channel又是从channelPool中获取的。
         * 这样就碰到了我们遇到的第一个问题：我们要如何把保存消息和删除消息。我这边处理方案是,将消息保存到如下结构体中:
         * # channelNumber channel的唯一标识: channel.getChannelNumber();
         * # deliverTag channel下递增的消息ID
         * # Message 消息体内容
         * Map<channelNumber , Map<deliverTag , Message>>
         *
         */
        try (
                // 1.获取连接对象
                Connection conn = RabbitMQConnectUtil.buildConnection();
                // 2.构建channel
                Channel channel = conn.createChannel()) {

            // 3.构建队列
            channel.queueDeclare(MessageConstant.CONFIRMS_QUEUE_CONSUMER, true, false, false, null);

            // 4.开启confirms
            channel.confirmSelect();
            ConcurrentSkipListSet<Long> confirmSet = new ConcurrentSkipListSet<>();
            // 5.设置 confirms的异步回调
            channel.addConfirmListener(new ConfirmListener() {
                /**
                 * 开启了confirm后，被确认的消息回调该方法（一旦消息被投递到所有匹配的队列之后，broker就会发送一个确认给生产者）
                 * @param deliveryTag：唯一标识id
                 * @param multiple
                 * @throws IOException
                 */
                @Override
                public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                    // 此外broker也可以设置basic.ack的multiple域，表示到这个序列号之前的所有消息都已经得到了处理
                    if (multiple) {
                        // 处理deliveryTag之前所有消息
                        confirmSet.headSet(deliveryTag).clear();
                    } else {
                        // 单条消息被处理
                        confirmSet.remove(deliveryTag);
                    }
                }

                /**
                 * 开启了confirm后，未被确认的消息回调该方法
                 * @param deliveryTag
                 * @param multiple
                 * @throws IOException
                 */
                @Override
                public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                    // 虽然消息没有被处理，但是也要清除掉避免，内存泄露问题
                    System.out.println("消息没有发送到Exchange，尝试重试或者持久化做补偿操作！");
                    if (multiple) {
                        confirmSet.headSet(deliveryTag + 1).clear();
                    } else {
                        confirmSet.remove(deliveryTag);
                    }
                }
            });

            // 6.开启return机制,必须在消息发送时携带一个参数mandatory=true
            channel.addReturnListener(new ReturnListener() {
                @Override
                public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    System.out.println("消息没有送达到Queue队列中，做其它的补偿措施！");
                }
            });

            // 7.设置消息持久化(参考MessageProperties)
            final AMQP.BasicProperties props = new AMQP.BasicProperties().builder()
                    .deliveryMode(2)
                    .build();

            // 8.发布消息(默认交换机就是空串),但是routingKey是必须要有的
            confirmSet.add(channel.getNextPublishSeqNo());
            channel.basicPublish("", MessageConstant.CONFIRMS_QUEUE_CONSUMER, true
                    , props, "Hello confirms!".getBytes());
            /**
             * 当我们发现不是每发一条消息都会ACK后，我们改了代码，在进行压测的时候，发现map中的消息还是清空不了。
             * 由于我们使用的是channelPool，channelPool进行销毁对象的时候，直接调用的channel.close.但是我们又开启confirm机制，就存在，当channel上还有消息未ack时，我们强行关闭channel，就导致channel不能进行ack，最后就体现在map中的数据不能清除。
             * 我们需要在close之前，调用channel.waitForConfirms()等待当前Channel上所有的消息都进行了ACK。
             */
            channel.waitForConfirms();

            // 这段代码的作用是为了查看图形界面的connections和channels
//            System.in.read();
        } catch (IOException | TimeoutException | InterruptedException e) {
            System.err.println(String.format("通讯方式【%s】: 发送消息失败！", "confirms"));
            e.printStackTrace();
        }
    }
}
