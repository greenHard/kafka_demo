package com.zhang.consumer.concurrent;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.stream.IntStream;

/**
 * 消费者 并发测试
 */
public class ConcurrentConsumerDemo {

    public static void main(String[] args) {
        // 1. 配置消费者客户端参数及创建相应的消费者实例。
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(loadProp());
        // 2. 多线程订阅主题
        IntStream.range(0, 2).forEach(i -> new Thread(() -> consumer.subscribe(Collections.singletonList("test_consumer1_topic")), "thread-" + i).start());
        // 3. 关闭消费者
        consumer.close();
    }

    private static Properties loadProp() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.153.1.117:9092,10.153.1.128:9092,10.153.1.131:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_concurrent-group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "concurrent-consumer-client");
        return props;
    }
}
