package org.ashish.kafkaavro.factory.iface;

import org.ashish.kafkaavro.common.JobConfig;
import org.ashish.kafkaavro.exception.JobProcessorException;
import org.ashish.kafkaavro.exception.MessageConsumerException;
import org.ashish.kafkaavro.exception.MessageWriterException;

import java.io.Serializable;
import java.util.Properties;

public abstract class IJob implements Serializable {

    protected JobConfig jobConfig;

    public IJob(Properties properties) throws  Exception {
        this.jobConfig = new JobConfig(properties);

    }

    abstract public void process()  throws JobProcessorException, MessageConsumerException, MessageWriterException;


}

