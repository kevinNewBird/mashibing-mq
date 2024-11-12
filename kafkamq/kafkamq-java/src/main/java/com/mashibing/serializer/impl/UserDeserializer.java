package com.mashibing.serializer.impl;

import com.mashibing.pojo.User;
import com.sun.xml.internal.ws.encoding.soap.DeserializationException;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * description: com.mashibing.serializer.impl
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/11/12
 * version: 1.0
 */
public class UserDeserializer implements Deserializer<User> {

    /**
     * 反序列化
     *
     * @param topic topic associated with the data
     * @param data  serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return
     */
    @Override
    public User deserialize(String topic, byte[] data) {
        if (null == data) {
            return null;
        }
        if (data.length < 8) {
            throw new SerializationException("Error data size.");
        }
        try {
            // 反序列化
            ByteBuffer buffer = ByteBuffer.wrap(data);
            int id = buffer.getInt();
            int nameSize = buffer.getInt();
            byte[] nameData = new byte[nameSize];
            buffer.get(nameData);
            return new User(id, new String(nameData, StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new DeserializationException("Error deserialize user." + e);
        }
    }
}
