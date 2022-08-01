package org.ashish.kafkaavro.factory.impl.job;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.ashish.kafkaavro.exception.JobProcessorException;
import org.ashish.kafkaavro.exception.MessageConsumerException;
import org.ashish.kafkaavro.exception.MessageWriterException;
import org.ashish.kafkaavro.factory.iface.IJob;
import org.ashish.kafkaavro.factory.provider.JobProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class DefaultJob extends IJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultJob.class.getName());

    public DefaultJob(Properties properties) throws Exception {
        super(properties);
    }

    @Override
    public void process() throws JobProcessorException, MessageConsumerException, MessageWriterException {

        try{
            JobProcessorFactory.getJobProcessor(this.jobConfig).process();
        }
        catch (Exception ex){
            LOGGER.error("Error while processing",ex);
            throw new JobProcessorException(ExceptionUtils.getStackTrace(ex));
        }
    }


}
