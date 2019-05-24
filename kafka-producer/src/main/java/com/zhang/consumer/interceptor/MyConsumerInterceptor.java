package com.zhang.consumer.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 消费者拦截器Demo
 */
public class MyConsumerInterceptor implements ConsumerInterceptor<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(MyConsumerInterceptor.class);
    /**
     * 过期时间10秒
     */
    private static final long EXPIRE_INTERVAL = 10 * 1000;

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        long now = System.currentTimeMillis();
        Map<TopicPartition, List<ConsumerRecord<String, String>>> newRecords = new HashMap<>();
        records.partitions().forEach(tp -> {
            List<ConsumerRecord<String, String>> topicRecords = records.records(tp);
            topicRecords.forEach(record -> {
                List<ConsumerRecord<String, String>> newTpRecords = new ArrayList<>();
                // 比较记录的时间和当前时间,如果超过指定的时间就舍弃
                if (now - record.timestamp() < EXPIRE_INTERVAL) {
                    newTpRecords.add(record);
                }
                if (!newTpRecords.isEmpty()) {
                    newRecords.put(tp, newTpRecords);
                }
            });
        });
        return new ConsumerRecords<>(newRecords);
    }

    @Override
    public void close() {

    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((p, o) -> {
            LOG.info("partition is {} ,offset is {}", p.partition(), o.offset());
        });
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
