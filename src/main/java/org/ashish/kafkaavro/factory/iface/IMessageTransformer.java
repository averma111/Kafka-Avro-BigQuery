package org.ashish.kafkaavro.factory.iface;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.ashish.kafkaavro.common.JobConfig;
import org.ashish.kafkaavro.common.JobContext;
import org.ashish.kafkaavro.exception.MessageTransfomerException;
import org.ashish.kafkaavro.model.TransformData;

import java.io.Serializable;

public abstract class IMessageTransformer implements Serializable {

    protected JobConfig jobConfig;
    protected JobContext jobContext;

    public IMessageTransformer(JobConfig jobConfig, JobContext jobContext){
        this.jobConfig = jobConfig;
        this.jobContext = jobContext;
    }

     abstract public TransformData transform(String groupColName, Dataset<Row> messageSchemaDs)
        throws MessageTransfomerException;
}
