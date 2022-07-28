package org.ashish.kafkaavro.factory.iface;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.ashish.kafkaavro.common.JobConfig;
import org.ashish.kafkaavro.common.JobContext;

import java.io.Serializable;
import org.ashish.kafkaavro.exception.MessageConsumerException;
import org.ashish.kafkaavro.model.MessageSchema;

public abstract class IMessageConsumer implements Serializable {

    protected JobConfig jobConfig;
    protected JobContext jobContext;

    public IMessageConsumer(JobConfig jobConfig, JobContext jobContext){
        this.jobConfig = jobConfig;
        this.jobContext =jobContext;
    }

    public abstract JavaDStream<? extends MessageSchema> consume() throws MessageConsumerException;

}
