package org.ashish.kafkaavro.common;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

public class JobContext implements Serializable {

    private transient JavaStreamingContext streamingContext;
    private SparkSession sparkSession;

    public JobContext(JavaStreamingContext streamingContext){
        this.streamingContext = streamingContext;
        this.sparkSession = JavaSparkSessionSingleton.getInstance(streamingContext.sparkContext().getConf());
    }

    public JavaStreamingContext getStreamingContext() {
        return streamingContext;
    }

    public void setStreamingContext(JavaStreamingContext streamingContext) {
        this.streamingContext = streamingContext;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }
}
