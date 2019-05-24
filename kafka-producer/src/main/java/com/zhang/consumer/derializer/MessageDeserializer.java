package com.zhang.consumer.derializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 自定义消息序列化器
 */
public class MessageDeserializer implements Deserializer<Message> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Message deserialize(String topic, byte[] data) {
        if (data == null)
            return null;
        if (data.length < 8)
            throw new SerializationException("The Message data bytes length should not be less than 8.");
        // 读取数据,先读取id,在读取length
        ByteBuffer buffer = ByteBuffer.wrap(data);
        // 先读取id
        buffer.getInt();
        int id = buffer.getInt();
        // 读取name长度
        int nameLength = buffer.getInt();
        byte[] nameBytes = new byte[nameLength];
        buffer.get(nameBytes);
        String name = new String(nameBytes);
        return new Message(id, name);
    }

    @Override
    public void close() {

    }
}
