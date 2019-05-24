package com.zhang.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 优雅的关闭消费者
 */
public class GentlestCloseConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(GentlestCloseConsumer.class);
    private static final AtomicBoolean isRunning = new AtomicBoolean(true);
    private static final AtomicInteger count = new AtomicInteger();

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(loadProp());
        consumer.subscribe(Collections.singletonList("test-consumer-commit-topic"));
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
                records.forEach(record -> {
                    //biz handler.
                    LOG.info("offset:{},value:{},key:{}", record.offset(), record.value(), record.key());
                    consumer.commitAsync();
                    // 消费10条之后优雅的关闭
                    if (count.incrementAndGet() == 10) {
                        isRunning.set(false);
                    }
                });
            }
        } catch (WakeupException e) {
            // ignore the error
        } catch (Exception e) {
            // do some logic process.
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }

    }

    private static Properties loadProp() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.153.1.117:9092,10.153.1.128:9092,10.153.1.131:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group9");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "gentlest-consumer-client");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}
