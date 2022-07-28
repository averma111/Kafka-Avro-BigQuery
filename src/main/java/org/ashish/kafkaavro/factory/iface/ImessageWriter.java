package org.ashish.kafkaavro.factory.iface;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.ashish.kafkaavro.common.JobConfig;
import org.ashish.kafkaavro.common.JobContext;
import org.ashish.kafkaavro.exception.MessageWriterException;

import java.io.Serializable;

public abstract class ImessageWriter implements Serializable {

    protected JobConfig jobConfig;
    protected JobContext jobContext;

    public ImessageWriter(JobConfig jobConfig, JobContext jobContext) {
        this.jobConfig = jobConfig;
        this.jobContext = jobContext;
    }

    public abstract  void write(String destDetails, Dataset<Row> dataToWrite) throws MessageWriterException;
}
