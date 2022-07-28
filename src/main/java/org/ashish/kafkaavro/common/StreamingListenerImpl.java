package org.ashish.kafkaavro.common;

import com.google.gson.JsonObject;
import org.apache.spark.api.python.WritableToJavaConverter;
import org.apache.spark.streaming.scheduler.StreamInputInfo;
import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.spark.streaming.scheduler.StreamingListenerStreamingStarted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static org.ashish.kafkaavro.util.Constants.*;


public class StreamingListenerImpl implements StreamingListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingListenerImpl.class.getName());
    private JobConfig jobConfig;

    public StreamingListenerImpl(JobConfig jobConfig){
        this.jobConfig = jobConfig;
    }

    @Override
    public void onStreamingStarted(StreamingListenerStreamingStarted streamingListenerStreamingStarted){
        LOGGER.info("StreamingListenerImpl :: Inside onStreamingStarted {}",streamingListenerStreamingStarted);
    }


    @Override
    public void onBatchStarted(StreamingListenerStreamingStarted streamingListenerStreamingStarted){
        String batchJobStartMs = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        this.jobConfig.getProperties().getProperty(BATCH_JOB_INITIATION_TIME,batchJobStartMs);

        Map<Object, StreamInputInfo> streamInfo = new HashMap<>(
                JavaConverters.mapAsJavaMapConverter(streamingListenerStreamingStarted.batchInfo().streamIdToInputInfo()).asJava());

        JsonObject jsonObject = new JsonObject();
        for(Map.Entry<Object, StreamInputInfo> streamInputInfoEntry:streamInfo.entrySet()){

        }
    }

}
