package org.ashish.kafkaavro.factory.provider;

import org.ashish.kafkaavro.common.JobConfig;
import org.ashish.kafkaavro.factory.iface.IJobProcessor;
import org.ashish.kafkaavro.factory.impl.processor.DefaultJobProcessor;

import static org.ashish.kafkaavro.util.Constants.*;

public class JobProcessorFactory {

    public static IJobProcessor getJobProcessor(JobConfig jobConfig){
        if(jobConfig.getProperties().getProperty(EVENT_SOURCE).equalsIgnoreCase(SOURCE_KAFKA)){

            return new DefaultJobProcessor(jobConfig);

        }

        return null;
    }
}
