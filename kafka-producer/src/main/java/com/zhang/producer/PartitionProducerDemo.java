package com.zhang.producer;


import com.zhang.producer.partition.MyPartitioner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 自定义分区生产者Demo
 */
public class PartitionProducerDemo {
    private final static Logger LOGGER = LoggerFactory.getLogger(PartitionProducerDemo.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = initProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
//        ProducerRecord<String, String> record = new ProducerRecord<>("partition-demo-topic", "ONE", "hello ONE");
//      ProducerRecord<String, String> record = new ProducerRecord<>("partition-demo-topic", "TWO", "hello TWO");
//      ProducerRecord<String, String> record = new ProducerRecord<>("partition-demo-topic", "THREE", "hello THREE");
        ProducerRecord<String, String> record = new ProducerRecord<>("partition-demo-topic", "FOUR", "hello FOUR");
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata recordMetadata = future.get();
        LOGGER.info("当前数据的partition为 {}", recordMetadata.partition());
        producer.flush();
        producer.close();
    }

    private static Properties initProps() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.153.1.78:9092,10.153.1.201:9092,10.153.0.208:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class.getName());
        return props;
    }
}
