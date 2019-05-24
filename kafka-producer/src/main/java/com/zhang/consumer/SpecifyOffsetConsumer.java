package com.zhang.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

/**
 * 指定位置消费
 */
public class SpecifyOffsetConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(SpecifyOffsetConsumer.class);

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(loadProp());
        consumer.subscribe(Collections.singletonList("test-consumer-commit-topic"));
        Set<TopicPartition> assignment;
        // 1. 保证可以拿到分区
        do {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        } while (assignment.size() == 0);

        // 2. 从每个分区的第5个位置开始消费
        for (TopicPartition tp : assignment) {
            consumer.seek(tp, 5);
        }

        // 3. 消费记录
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            records.forEach(record -> {
                //biz handler.
                LOG.info("offset:{},value:{},key:{}", record.offset(), record.value(), record.key());
                consumer.commitAsync();
            });
        }
    }


    private static Properties loadProp() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.153.1.117:9092,10.153.1.128:9092,10.153.1.131:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "specify_offset_group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "specify-offset-client");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }
}
