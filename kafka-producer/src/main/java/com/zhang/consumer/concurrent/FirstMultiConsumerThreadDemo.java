package com.zhang.consumer.concurrent;

import com.zhang.consumer.KafkaConsumerPositionDemo;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

/**
 * 第一种线程消费方式
 */
public class FirstMultiConsumerThreadDemo {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerPositionDemo.class);

    public static void main(String[] args) {
        IntStream.range(0, 2).forEach(i -> new KafkaConsumerThread(loadProp(),"test_consumer1_topic").start());
        LOG.error("not to here");
    }

    private static Properties loadProp() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.153.1.117:9092,10.153.1.128:9092,10.153.1.131:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "First-Multi-Consumer-group-1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}
