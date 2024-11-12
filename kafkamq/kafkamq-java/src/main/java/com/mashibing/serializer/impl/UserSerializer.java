package com.mashibing.serializer.impl;

import com.mashibing.pojo.User;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * description: com.mashibing.serializer.impl
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/11/12
 * version: 1.0
 */
public class UserSerializer implements Serializer<User> {

    /**
     * 序列化
     * @param topic topic associated with data
     * @param data typed data
     * @return
     */
    @Override
    public byte[] serialize(String topic, User data) {
        if (null == data) {
            return null;
        }
        try {
            int nameSize = 0;
            byte[] nameData = new byte[0];
            if (data.getName() != null) {
                nameData = data.getName().getBytes(StandardCharsets.UTF_8);
                nameSize = data.getName().length();
            }
            // id的长度4个字节，字符串的长度描述4个字节
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + nameSize);
            buffer.putInt(data.getId());
            buffer.putInt(nameSize);
            buffer.put(nameData);
            return buffer.array();
        } catch (Exception e) {
            throw new SerializationException("Error serialize user!" + e);
        }
    }
}
