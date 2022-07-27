package org.ashish.kafkaavro.consume;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.ashish.kafkaavro.config.ConfigProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Collections;
import java.util.Properties;

public class ConsumeRecords {

    public final static Logger LOGGER = LoggerFactory.getLogger(ConsumeRecords.class);

    public static Consumer<String, String> consumeRecords() {
        final Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  ConfigProperties.getInstance().getProperty("KAFKA_BROKERS"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, ConfigProperties.getInstance().getProperty("GROUP_ID_CONFIG"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ConfigProperties.getInstance().getProperty("KEY_DESERIALIZER"));
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,ConfigProperties.getInstance().getProperty("VALUE_DESERIALIZER"));
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, ConfigProperties.getInstance().getProperty("MAX_POLL_RECORDS"));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ConfigProperties.getInstance().getProperty("OFFSET_RESET_LATEST"));

        final Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(ConfigProperties.getInstance().getProperty("TOPIC_NAME")));
        LOGGER.info("Consumer object is created");
        return consumer;

    }
}
