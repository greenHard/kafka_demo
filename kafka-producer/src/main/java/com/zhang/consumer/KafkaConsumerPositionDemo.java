package com.zhang.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerPositionDemo {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerPositionDemo.class);


    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(loadProp());
        TopicPartition tp = new TopicPartition("test_consumer2_topic", 0);
        consumer.assign(Arrays.asList(tp));
        // 当前消费到的位移
        long lastConsumedOffset = -1;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            if (records.isEmpty()) {
                break;
            }
            List<ConsumerRecord<String, String>> partitionRecords = records.records(tp);
            lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            // 同步提交消费位移
            consumer.commitSync();
        }
        LOG.info("consumed offset is {}", lastConsumedOffset);
        OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
        LOG.info("consumed offset is {}", offsetAndMetadata.offset());
        long position = consumer.position(tp);
        LOG.info("the offset of the next record is {}", position);
    }

    private static Properties loadProp() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.153.1.117:9092,10.153.1.128:9092,10.153.1.131:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group4");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-position-client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        return props;
    }
}
