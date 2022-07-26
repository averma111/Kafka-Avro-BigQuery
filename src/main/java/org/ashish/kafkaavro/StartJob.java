package org.ashish.kafkaavro;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.ashish.kafkaavro.Interface.IKafkaConstants;
import org.ashish.kafkaavro.consume.ConsumeRecords;
import org.ashish.kafkaavro.produce.ProduceRecords;
import java.util.UUID;

import java.util.concurrent.ExecutionException;

/**
 * A Kafka Application
 */
public class StartJob {

    /**
     * A main() so we can easily run these routing rules in our IDE
     */
    public static void main(String[] args) {

       //runProducer();
       runConsumer();

    }

    public static void runProducer() {

        Producer<String, String> producer = ProduceRecords.produceRecords();

        for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(IKafkaConstants.TOPIC_NAME
                    ,UUID.randomUUID().toString(),
                    "This is record " + index);
            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
            } catch (ExecutionException | InterruptedException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }
        }
    }

    public static void runConsumer() {
        Consumer<String, String> consumer = ConsumeRecords.consumeRecords();

        int noMessageToFetch = 0;

        while (true) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
            if (consumerRecords.count() == 0) {
                noMessageToFetch++;
                if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    break;
                else
                    continue;
            }

            consumerRecords.forEach(record -> {
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
            });
            consumer.commitAsync();
        }
        consumer.close();
    }

}

