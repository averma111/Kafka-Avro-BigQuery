package org.ashish.kafkaavro.factory.provider;

import org.ashish.kafkaavro.common.JobConfig;
import org.ashish.kafkaavro.common.JobContext;
import org.ashish.kafkaavro.exception.SystemException;
import org.ashish.kafkaavro.factory.iface.IMessageConsumer;
import org.ashish.kafkaavro.factory.impl.consumer.KafkaMessageConsumer;

import static org.ashish.kafkaavro.util.Constants.*;


public class MessageConsumerFactory {

    public static IMessageConsumer getMessageConsumer(JobConfig jobConfig, JobContext jobContext) throws Exception {

        if(jobConfig.getProperties().getProperty(EVENT_SOURCE).equalsIgnoreCase(SOURCE_KAFKA)){
            return new KafkaMessageConsumer(jobConfig,jobContext);
        }

        throw new SystemException("Invalid configuration for consumer");

    }
}
