package com.zhang.producer.serializer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

public class MySerializerProducerDemo {
    private final static Logger LOGGER = LoggerFactory.getLogger(MySerializerProducerDemo.class);

    public static void main(String[] args) {
        Properties properties = initProps();
        KafkaProducer<String, Message> producer = new KafkaProducer<>(properties);
        IntStream.range(0, 10).forEach(i ->
        {
            ProducerRecord<String, Message> record = new ProducerRecord<>("send-demo-topic", String.valueOf(i), new Message(10, "test"));
            Future<RecordMetadata> future = producer.send(record);
            try {
                RecordMetadata metaData = future.get();
                LOGGER.info("The message is send done and the key is {},offset {}", i, metaData.offset());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
        producer.flush();
        producer.close();
    }

    private static Properties initProps() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.153.1.117:9092,10.153.1.128:9092,10.153.1.131:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class.getName());
        return props;
    }
}
