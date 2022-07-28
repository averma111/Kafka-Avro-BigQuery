package org.ashish.kafkaavro.driver;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.ashish.kafkaavro.config.ConfigProperties;
import org.ashish.kafkaavro.consume.ConsumeRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartConsumer {

    public static  final Logger LOGGER = LoggerFactory.getLogger(StartConsumer.class);
    public static void main(String[] args) {
        runConsumer();

    }
    public static void runConsumer() {
        Consumer<String, String> consumer = ConsumeRecords.consumeRecords();
        int noMessageToFetch = 0;
        while (true) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
            if (consumerRecords.count() == 0) {
                noMessageToFetch++;
                if (noMessageToFetch > Integer.parseInt(ConfigProperties.getInstance().getProperty("MAX_NO_MESSAGE_FOUND_COUNT")))
                    break;
                else
                    continue;
            }
            consumerRecords.forEach(record -> {
                LOGGER.info("Record Key " + record.key());
                LOGGER.info("Record value " + record.value());
                //System.out.println("Record partition " + record.partition());
                //System.out.println("Record offset " + record.offset());
            });
            consumer.commitAsync();
        }
        consumer.close();
    }

}

