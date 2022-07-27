package org.ashish.kafkaavro.produce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.ashish.kafkaavro.config.ConfigProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProduceRecords {

    public static final Logger LOGGER = LoggerFactory.getLogger(ProduceRecords.class);
    public static Producer<String, String> produceRecords() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigProperties.getInstance().getProperty("KAFKA_BROKERS"));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, ConfigProperties.getInstance().getProperty("CLIENT_ID"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ConfigProperties.getInstance().getProperty("KEY_SERIALIZER"));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,ConfigProperties.getInstance().getProperty("VALUE_SERIALIZER"));

        LOGGER.info("Producer Object is created");
        return new KafkaProducer<String, String>(props);
    }

}
