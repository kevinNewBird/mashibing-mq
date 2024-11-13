package com;

/**
 * description: 计算consumer的消费的offset提交保存位置
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/11/13
 * version: 1.0
 */
public class ConsumerOffsets {

    public static void main(String[] args) {
        String groupId = "user001";
        // __consumer_offsets默认会有50个，计算的简单公式如下
        System.out.println(Math.abs(groupId.hashCode() % 50));
    }
}
