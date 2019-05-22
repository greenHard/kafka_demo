package com.zhang.producer.serializer;

import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 自定义消息序列化器
 */
public class MessageSerializer implements Serializer<Message> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Message data) {
        if (data == null)
            return null;
        int id = data.getId();
        String name = data.getName();
        int nameLength = 0;
        if (name != null && !name.isEmpty()) {
            nameLength = name.length();
        }
        try {
            // 开启一个内存区间
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + 4 + nameLength);
            buffer.putInt(4);
            buffer.putInt(id);
            buffer.putInt(4);
            buffer.put(name.getBytes("UTF-8"));
            return buffer.array();
        } catch (UnsupportedEncodingException e) {
            // 编码不支持异常
            e.printStackTrace();
        }

        return new byte[0];
    }

    @Override
    public void close() {

    }
}
