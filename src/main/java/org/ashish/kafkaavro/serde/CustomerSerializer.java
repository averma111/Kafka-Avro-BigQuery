package org.ashish.kafkaavro.serde;

import org.apache.kafka.common.serialization.Serializer;
import org.ashish.kafkaavro.model.CustomerObject;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CustomerSerializer implements Serializer<CustomerObject> {

    public static final Logger LOGGER = LoggerFactory.getLogger(CustomerSerializer.class);
    @Override
    public void configure(Map<String, ?> configs, boolean isKey){

    }

    @Override
    public byte[] serialize(String topic,CustomerObject data){
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(data).getBytes();
        }catch (Exception exception){
            System.out.println("Error in serializing object" + data);
        }
        LOGGER.info("Sending the serialized bytes");
        return retVal;
    }

    @Override
    public void close(){

    }
 }
