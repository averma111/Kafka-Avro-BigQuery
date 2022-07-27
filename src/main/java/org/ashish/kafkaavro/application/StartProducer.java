package org.ashish.kafkaavro.application;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.ashish.kafkaavro.Interface.IKafkaConstants;
import org.ashish.kafkaavro.produce.ProduceRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import java.util.concurrent.ExecutionException;

public class StartProducer {

    public static  final Logger LOGGER = LoggerFactory.getLogger(StartProducer.class);
    public static void main(String[] args) {
       runProducer();
    }
    public static void runProducer() {
        Producer<String, String> producer = ProduceRecords.produceRecords();
        Faker faker = new Faker();
        for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(IKafkaConstants.TOPIC_NAME
                    ,UUID.randomUUID().toString(),faker.name().fullName());
            try {
                RecordMetadata metadata = producer.send(record).get();
                LOGGER.info("Record sent with key " + index + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
            } catch (ExecutionException | InterruptedException e) {
                LOGGER.error("Error in sending record");
                LOGGER.error(String.valueOf(e));
            }
        }
    }
}

