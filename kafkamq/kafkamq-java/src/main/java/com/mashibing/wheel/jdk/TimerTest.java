package com.mashibing.wheel.jdk;

import org.junit.Test;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

/**
 * description：jdk原生定时类Timer，使用的单线程(TimerThread)的方式。任务的执行是依据延时时间来的，而不是加入时间。
 * <br/>
 * 单线程其存在一个问题，多个任务放入时，如果前一个任务没有执行完，后一个任务即使达到了执行时间也不会执行。
 * 适合简单任务，且执行时间短的任务
 * <br/>
 * 引入多线程定时任务来解决该问题，即ScheduleThreadPool
 *
 * @author zhaosong
 * @version 1.0
 * @company 北京海量数据有限公司
 * @date 2024/11/23 15:30
 */
public class TimerTest {

    @Test
    public void test1() throws IOException {
        Timer timer0 = new Timer();
        timer0.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println("[x1] scheduled timer task is running!");
                // 死循环
                while (true) {
                }
            }
        }, 300);


        // 这个任务永远也不会被执行到，因为Timer的定时是不论任务的加入先后
        // 而是看任务具体执行的时间，时间靠前的先执行（即优先队列ProprityQueue）。
        // 将延时设置到从400改为200，会被执行一次。但是下一次定时时间500到来时，同样不会执行，因为上一个任务阻塞
        timer0.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println("[x2] scheduled timer task is running!");
            }
        }, 400, 500);

        // 保证定时线程执行
        System.in.read();
    }

}
