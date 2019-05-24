package com.zhang.consumer.derializer;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 反序列化器Demo
 */
public class MyDeserializerConsumerDemo {
    private final static Logger LOGGER = LoggerFactory.getLogger(MyDeserializerConsumerDemo.class);
    private static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static void main(String[] args) {
        KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(loadProp());
        consumer.subscribe(Collections.singletonList("send-demo-topic"));
        while (isRunning.get()) {
            ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(r -> LOGGER.info("message is {}", r));
        }
    }

    private static Properties loadProp() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.153.1.117:9092,10.153.1.128:9092,10.153.1.131:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group10");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-deserializer-client");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}
