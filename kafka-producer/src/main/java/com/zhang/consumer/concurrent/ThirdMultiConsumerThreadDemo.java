package com.zhang.consumer.concurrent;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 第三种线程消费方式
 */
public class ThirdMultiConsumerThreadDemo {
    private static final Logger LOG = LoggerFactory.getLogger(ThirdMultiConsumerThreadDemo.class);

    public static void main(String[] args) throws InterruptedException {
        // 获取系统可并发的进程数
        new KafkaConsumerThread(loadProp(),"test_consumer1_topic",Runtime.getRuntime().availableProcessors());
        Thread.currentThread().join();
    }


    public static class KafkaConsumerThread extends Thread {
        private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerThread.class);
        private KafkaConsumer<String, String> kafkaConsumer;
        private ExecutorService executorService;

        public KafkaConsumerThread(Properties props, String topic, int threadNumber) {
            this.kafkaConsumer = new KafkaConsumer<>(props);
            this.kafkaConsumer.subscribe(Arrays.asList(topic));
            executorService = new ThreadPoolExecutor(threadNumber, threadNumber, 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(200), new ThreadPoolExecutor.CallerRunsPolicy());
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    if (!records.isEmpty()) {
                        LOG.info("async handle message...");
                        executorService.submit(new RecordsHandler(records));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                kafkaConsumer.close();
            }
        }

        /**
         * 消息处理器
         */
        public static class RecordsHandler extends Thread{
            private static final Logger LOG = LoggerFactory.getLogger(RecordsHandler.class);

            public final ConsumerRecords<String, String> records;

            public RecordsHandler(ConsumerRecords<String, String> records) {
                this.records = records;
            }

            @Override
            public void run(){
                records.forEach(r ->{
                    // biz handler
                    LOG.info("consumer message the partition is {}, the offset is {}", r.partition(), r.offset());
                });
            }
        }
    }

    private static Properties loadProp() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.153.1.117:9092,10.153.1.128:9092,10.153.1.131:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Third-Multi-Consumer-group-1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

}
