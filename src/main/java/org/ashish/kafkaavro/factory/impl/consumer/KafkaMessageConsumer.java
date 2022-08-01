package org.ashish.kafkaavro.factory.impl.consumer;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.ashish.kafkaavro.common.JobConfig;
import org.ashish.kafkaavro.common.JobContext;
import org.ashish.kafkaavro.exception.MessageConsumerException;
import org.ashish.kafkaavro.factory.iface.IMessageConsumer;
import org.ashish.kafkaavro.model.MessageSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMessageConsumer extends IMessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageConsumer.class.getName());

    public KafkaMessageConsumer(JobConfig jobConfig, JobContext jobContext){
        super(jobConfig,jobContext);
    }

    @Override
    public JavaDStream<? extends MessageSchema> consume() throws MessageConsumerException {
        return null;
    }
}
