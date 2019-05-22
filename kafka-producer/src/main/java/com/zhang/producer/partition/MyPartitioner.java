package com.zhang.producer.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义Partitioner
 * {@link Partitioner}
 */
public class MyPartitioner implements Partitioner {
    private final String ONE = "ONE";
    private final String TWO = "TWO";
    private final String THREE = "THREE";

    /**
     * Compute the partition for the given record.
     *
     * @param topic      The topic name
     * @param key        The key to partition on (or null if no key)
     * @param keyBytes   serialized key to partition on (or null if no key)
     * @param value      The value to partition on or null
     * @param valueBytes serialized value to partition on or null
     * @param cluster    The current cluster metadata
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (keyBytes == null || keyBytes.length == 0) {
            throw new IllegalArgumentException("The key is required for BIZ.");
        }
        switch (key.toString().toUpperCase()) {
            case ONE:
                return 0;
            case TWO:
                return 1;
            case THREE:
                return 2;
            default:
                throw new IllegalArgumentException("The key is invalid.");
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
