package com.mashibing.partitioner.impl;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * description: com.mashibing.partitioner.impl
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/11/12
 * version: 1.0
 */
public class ValuePartitioner implements Partitioner {

    /**
     * 分区算法
     *
     * @param topic      The topic name
     * @param key        The key to partition on (or null if no key)
     * @param keyBytes   The serialized key to partition on( or null if no key)
     * @param value      The value to partition on or null
     * @param valueBytes The serialized value to partition on or null
     * @param cluster    The current cluster metadata
     * @return
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 获取分区列表
        List<PartitionInfo> partitionList = cluster.partitionsForTopic(topic);
        // 分区数量
        int partitionNum = partitionList.size();

        // 计算分区
        return Utils.toPositive(Utils.murmur2(valueBytes)) % partitionNum;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
