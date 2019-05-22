package com.zhang.producer.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 前缀拦截器
 */
public class ProducerInterceptorPrefix implements ProducerInterceptor<String, String> {
    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 给value前面加个前缀
        String newValue = "producer-" + record.value();
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), newValue, record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(exception == null){
            sendSuccess++;
        } else{
            sendFailure++;
        }
    }

    @Override
    public void close() {
        // 统计发送成功率
        double successRatio = (double)sendSuccess / (sendFailure + sendSuccess);
        System.out.println("[INFO] 发送成功率=" + String.format("%f", successRatio * 100) + "%");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
