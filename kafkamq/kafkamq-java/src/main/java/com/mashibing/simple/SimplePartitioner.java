package com.mashibing.simple;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * description: kafka分区器，默认是hash分区器
 * company: 北京海量数据有限公司
 * create by: zhaosong 2023/3/31
 * version: 1.0
 */
public class SimplePartitioner implements Partitioner {
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
