package com.mashibing.wheel.netty;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * description：基于时间轮的定时任务(netty提供)
 * <br/>
 * HashedWheelTimer是一个粗略的定时实现，之所以称之为粗略的实现是因为该时间轮并没有严格的准时执行定时任务，
 * 而是在每隔一个时间间隔之后的时间节点执行，并执行当前时间节点之前到期的定时任务
 *
 * @author zhaosong
 * @version 1.0
 * @company 北京海量数据有限公司
 * @date 2024/11/24 11:10
 */
public class HashedWheelTimerTest {

    @Test
    public void test1() {
        CountDownLatch latch = new CountDownLatch(2);
        // 1.创建Timer对象
        HashedWheelTimer timer = new HashedWheelTimer();
        System.out.printf("%s 准备加入任务！！！%n", LocalDateTime.now());
        // 2.提交一个任务，让其5s后执行
        Timeout timeout1 = timer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                System.out.printf("%s [x] 5s 后执行该任务！！！%n", LocalDateTime.now());
                latch.countDown();
            }
        }, 5, TimeUnit.SECONDS);

        // 3.再次提交一个任务，让其在10s后执行
        Timeout timeout2 = timer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                System.out.printf("%s [x] 10s 后执行该任务！！！%n", LocalDateTime.now());
                latch.countDown();
            }
        }, 10, TimeUnit.SECONDS);

        // 4.取消掉那个5s 后执行的任务
        if (!timeout1.isExpired()) {
            timeout1.cancel();
        }

        // 5.原来那个5s后执行的任务已经取消了。这里我们反悔了，让其在3s后执行
        timer.newTimeout(timeout1.task(), 3, TimeUnit.SECONDS);

        // 6.等待退出
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                latch.await();
                System.out.println("END!!!");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
        System.exit(0);
    }
}
