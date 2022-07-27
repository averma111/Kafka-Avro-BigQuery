package org.ashish.kafkaavro.util;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CustomerPartitioner implements Partitioner {

    public static final Logger LOGGER = LoggerFactory.getLogger(CustomerPartitioner.class);

    private static final int PARTITION_COUNT=50;

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int keyInt=Integer.parseInt(key.toString());
        LOGGER.info("Creating the partition counts");
        return keyInt % PARTITION_COUNT;
    }

    @Override
    public void close() {

    }
}
