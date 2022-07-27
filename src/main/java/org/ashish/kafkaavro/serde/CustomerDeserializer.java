package org.ashish.kafkaavro.serde;


import org.apache.kafka.common.serialization.Deserializer;
import org.ashish.kafkaavro.model.CustomerObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CustomerDeserializer implements Deserializer<CustomerObject> {

    public static  final Logger LOGGER = LoggerFactory.getLogger(CustomerDeserializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public CustomerObject deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        CustomerObject object = null;
        try {
            object = mapper.readValue(data, CustomerObject.class);
        } catch (Exception exception) {
            System.out.println("Error in deserializing bytes " + exception);
        }
        LOGGER.info("Sending the deserialized object");
        return object;
    }
    @Override
    public void close() {
    }
}