package org.ashish.kafkaavro.util;

import org.apache.kafka.common.serialization.Serializer;
import org.ashish.kafkaavro.model.CustomerObject;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Map;

public class CustomerSerializer implements Serializer<CustomerObject> {

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
        return retVal;
    }

    @Override
    public void close(){

    }
 }
