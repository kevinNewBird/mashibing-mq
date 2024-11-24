package com.mashibing.wheel.jdk;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * description：多线程定时任务jdk原生实现，解决Timer的技术痛点
 *
 * @author zhaosong
 * @version 1.0
 * @company 北京海量数据有限公司
 * @date 2024/11/23 16:12
 */
public class ScheduledThreadPoolTest {

    @Test
    public void test1() {
        int concurrent = 3;
        CountDownLatch latch = new CountDownLatch(concurrent);

        // 1.定时周期任务
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(concurrent);
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                System.out.println(Thread.currentThread().getName() + " ::: 小飞棍来喽");
                latch.countDown();
            }
        }, 3, 2, TimeUnit.SECONDS);

        // 2.等待退出
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
