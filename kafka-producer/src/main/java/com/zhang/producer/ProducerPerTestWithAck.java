package com.zhang.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

/**
 * Producer性能测试 使用 ack
 */
public class ProducerPerTestWithAck {
    private final static Logger LOGGER = LoggerFactory.getLogger(AsyncProducerDemo.class);

    public static void main(String[] args) {
        Properties properties = initProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        long start = System.currentTimeMillis();
        IntStream.range(0, 100000).forEach(i -> {
            ProducerRecord<String, String> record = new ProducerRecord<>("ack-demo-topic", String.valueOf(i), "hello " + i);
            producer.send(record);
        });
        producer.flush();
        producer.close();
        long current = System.currentTimeMillis();
        LOGGER.info("total const {} ms", (current - start));
    }

    /**
     * 100000
     * ack=0 4737 ms
     * ack=1 6733 ms
     * ack=all 8144 ms
     *
     * ack=0 snappy 5183 ms
     * ack=1 snappy 5665 ms
     * ack=all snappy 6325ms
     */
    private static Properties initProps() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.153.1.78:9092,10.153.1.201:9092,10.153.0.208:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        return props;
    }
}
