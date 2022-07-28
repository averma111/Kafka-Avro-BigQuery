package org.ashish.kafkaavro.factory.iface;

import org.ashish.kafkaavro.common.JobConfig;
import org.ashish.kafkaavro.common.JobContext;
import org.ashish.kafkaavro.util.SparkUtils;

import java.io.Serializable;

public abstract class IJobProcessor implements Serializable {

    protected JobConfig jobConfig;
    protected JobContext jobContext;

    public IJobProcessor(JobConfig jobConfig){
        this.jobConfig = jobConfig;
        this.jobContext = new JobContext(SparkUtils.createStreamingContext(jobConfig));
    }


    public abstract void process() throws  Exception;
}
