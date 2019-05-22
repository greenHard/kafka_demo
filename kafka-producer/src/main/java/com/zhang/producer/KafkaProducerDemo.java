package com.zhang.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * kafka 生产者demo
 * <p>
 * 1. 配置生产者客户端参数及创建相应的生产者实例。
 * 2. 构建待发送的消息。
 * 3. 发送消息。
 * 4. 关闭生产者实例。
 */
public class KafkaProducerDemo {
    public static final String brokerList = "10.153.1.78:9092";
    public static final String topic = "topicDemo";

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("client.id", "producer.client.id.demo");
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "Hello, Kafka!");
            producer.send(record);
            producer.flush();
            producer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
