package org.ashish.kafkaavro.common;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.spark.streaming.scheduler.*;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.ashish.kafkaavro.util.Constants.*;


public class StreamingListenerImpl implements StreamingListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingListenerImpl.class.getName());
    private final JobConfig jobConfig;

    public StreamingListenerImpl(JobConfig jobConfig) {
        this.jobConfig = jobConfig;
    }

    @Override
    public void onStreamingStarted(StreamingListenerStreamingStarted streamingListenerStreamingStarted) {
        LOGGER.info("StreamingListenerImpl :: Inside onStreamingStarted {}", streamingListenerStreamingStarted);
    }


    @Override
    public void onBatchStarted(StreamingListenerStreamingStarted streamingListenerStreamingStarted) {
        String batchJobStartMs = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        this.jobConfig.getProperties().getProperty(BATCH_JOB_INITIATION_TIME, batchJobStartMs);

        Map<Object, StreamInputInfo> streamInfo = new HashMap<>(
                JavaConverters.mapAsJavaMapConverter(streamingListenerStreamingStarted.batchInfo().streamIdToInputInfo()).asJava());

        JsonObject jsonObject = new JsonObject();
        for (Map.Entry<Object, StreamInputInfo> streamInfoEntry : streamInfo.entrySet()) {

            Map<String, Object> metaData = new HashMap<>(
                    JavaConverters.mapAsJavaMapConverter(streamInfoEntry.getValue().metadata().asJava()));
            ArrayList<OffsetRange> offsetArray = new ArrayList<>(
                    JavaConverters.asJavaCollectionConverter(
                            (List<OffsetRange>) metaData.get("offsets")).asJavaCollection());


            for (OffsetRange offsetRange : offsetArray) {
                jsonObject.addProperty(String.valueOf(offsetRange.partition()), offsetRange.fromOffset());
            }
        }

        LOGGER.info("StreamingListenerImpl :: onBatchStarted :: starting offset info ::" + new Gson.toJson(jsonObject));
    }

    @Override
    public void onBatchComplete(StreamingListenerBatchCompleted streamingListenerBatchCompleted) {
        String batchJobStartMs = this.jobConfig.getProperties().getProperty(BATCH_JOB_INITIATION_TIME);
        JsonObject offsetMetaJson = new JsonObject();

        if (!Boolean.getBoolean(String) this.jobConfig.getProperties().getOrDefault(IS_JOB_FAILED, "false")){

            try {
                Map<Object, StreamInputInfo> streamInputInfo = new HashMap<>(
                        JavaConverters.mapAsJavaMapConverter(streamingListenerBatchCompleted.batchInfo().streamIdToInputInfo().asJava());

                for (Map.Entry<Object, StreamInputInfo> streamInputInfoEntry : streamInputInfo.entrySet()) {

                    Map<String, Object> metaData = new HashMap<>(
                            JavaConverters.mapAsJavaMapConverter(streamInputInfoEntry.getValue().metadata().asJava()));

                    ArrayList<OffsetRange> offsetAarry = new ArrayList<>(
                            JavaConverters.asJavaCollectionConverter((List<OffsetRange>) metaData.get("offset")).asJavaCollection());

                    for (OffsetRange offsetRange : offsetAarry) {
                        offsetMetaJson.addProperty(String.valueOf(offsetRange.partition()), offsetRange.untilOffset());
                    }
                }
                String offsetMeta = new Gson.toJson(offsetMetaJson);
                GCSUtils.createOrUpdateOffsetInfoToGCS(this.jobConfig, offsetMeta);
                LOGGER.info("StremaingListenerImpl :: onBatchCompleted :: Offset info written to GCS :: {}", offsetMeta);
            } catch (Exception ex) {
                LOGGER.error("StremaingListenerImpl :: onBatchCompleted :: Error while writing offset to GCS", ex);
            }

        }

    }

    @Override
    public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {

    }

    @Override
    public void onReceiverError(StreamingListenerReceiverError receiverError) {

    }

    @Override
    public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {

    }

    @Override
    public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {

    }

    @Override
    public void onOutputOperationStarted(StreamingListenerOutputOperationStarted outputOperationStarted) {

    }

    @Override
    public void onOutputOperationCompleted(StreamingListenerOutputOperationCompleted outputOperationCompleted) {

    }
}
