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

/**
 * 异步+同步 提交
 */
public class CommitAsyncAndSyncCommit {
    private static final Logger LOG = LoggerFactory.getLogger(CommitAsyncAndSyncCommit.class);

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(loadProp());
        consumer.subscribe(Collections.singletonList("test-consumer-commit-topic"));
        try {
            for (; ; ) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                records.forEach(record -> {
                    //biz handler.
                    LOG.info("offset:{},value:{},key:{}", record.offset(), record.value(), record.key());
                    consumer.commitAsync();
                });

            }
        } finally {
            try {
                // 避免出现异常,需同步提交一次
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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group8");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-commit-sync-async-client");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}
