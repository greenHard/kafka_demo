package com.zhang.consumer;

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
 * 基础Consumer
 * 1. 配置消费者客户端参数及创建相应的消费者实例。
 * 2. 订阅主题。
 * 3. 拉取消息并消费。
 * 4. 提交消费位移。
 * 5. 关闭消费者实例。
 */
public class SimpleConsumerDemo {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleConsumerDemo.class);

    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static void main(String[] args) {
        // 1. 配置消费者客户端参数及创建相应的消费者实例。
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(loadProp());
        // 2. 订阅主题
        consumer.subscribe(Collections.singletonList("test_consumer1_topic"));
        // 3. 拉取消息并消费。
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record ->
                {
                    //biz handler.
                    LOG.info("offset:{}", record.offset());
                    LOG.info("value:{}", record.value());
                    LOG.info("key:{}", record.key());
                });
            }
        } catch (Exception e) {
            LOG.error("出现了异常. ", e);
        } finally {
            // 5. 关闭消费者实例。
            consumer.close();
        }

    }

    private static Properties loadProp() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.153.1.117:9092,10.153.1.128:9092,10.153.1.131:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group4");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-client");
        return props;
    }
}
