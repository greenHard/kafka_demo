package com.zhang.consumer.concurrent;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * 实现了{@link Thread}的Kafka消费者
 */
public class KafkaConsumerThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerThread.class);

    private KafkaConsumer<String, String> kafkaConsumer;

    public KafkaConsumerThread(Properties props, String topic) {
        this.kafkaConsumer = new KafkaConsumer<>(props);
        this.kafkaConsumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    // biz handler
                    LOG.info("consumer message the partition is {}, the offset is {}", record.partition(), record.offset());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }
}
