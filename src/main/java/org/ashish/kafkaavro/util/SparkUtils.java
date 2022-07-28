package org.ashish.kafkaavro.util;

import javafx.util.Duration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.ashish.kafkaavro.common.JobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.ashish.kafkaavro.util.Constants.*;

public class SparkUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkUtils.class.getName());

    public static String getCheckPointLocation(JobConfig jobConfig) {
        return jobConfig.getProperties().getProperty(SPARK_STREAMING_CHECKPOINT_LOCATION);
    }

    public static String getAppName(JobConfig jobConfig) {
        return jobConfig.getProperties().getProperty(TENANT).toUpperCase();
    }

    public static JavaStreamingContext createJavaStreamingContext(JobConfig jobConfig) {
        boolean isKafkaSource = jobConfig.getProperties().getProperty(EVENT_SOURCE).equalsIgnoreCase(SOURCE_KAFKA);

        SparkConf sparkConf = new SparkConf()
                .setAppName(getAppName(jobConfig))
                .set("spark.sql.caseSensitive", "true");

        if (isKafkaSource) {
            sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true");

            if (jobConfig.getProperties().getProperty(SPARK_STREAMING_MAX_RATE_PER_PARTITION) != null) {
                sparkConf.set(SPARK_STREAMING_MAX_RATE_PER_PARTITION, jobConfig.getProperties().getProperty(SPARK_STREAMING_MAX_RATE_PER_PARTITION));
            }

        } else {
            sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        }

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Duration.seconds(Integer.parseInt(
                String.valueOf(jobConfig.getProperties().getProperty(SPARK_STREAMING_BATCH_DURATION,"900)")))));

        if (isKafkaSource) {
            streamingContext.addStreamingListener(new StreamingListenerImpl(jobConfig));
        }

        LOGGER.warn("Cluster spark streaming context created.");

        return streamingContext;
    }

    public static JavaStreamingContext getOrCreateJavaStreamingContext(JobConfig jobConfig) {
        return JavaStreamingContext.getOrCreate(SparkUtils.getCheckPointLocation(jobConfig),
                (Function0<JavaStreamingContext>) () -> SparkUtils.createJavaStreamingContext(jobConfig));
    }

    public static JavaStreamingContext getLocalSparkStreamingSession(JobConfig jobConfig) {
        SparkConf sparkConf = new SparkConf()
                .setAppName(getAppName(jobConfig))
                .setMaster("local[*]")
                .set("spark.sql.caseSensitive", "true");

        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(sparkConf));
        JavaStreamingContext streamingContext = new JavaStreamingContext(javaSparkContext, Duration.seconds(Integer.parseInt(
                String.valueOf(jobConfig.getProperties().getProperty(SPARK_STREAMING_BATCH_DURATION,"900")))));

        LOGGER.warn("Local spark streaming context created");
        return streamingContext;
    }

    public static JavaStreamingContext createStreamingContext(JobConfig jobConfig) {
        String environment = jobConfig.getProperties().getProperty(ENV);
        if (environment.equals("local")) {
            LOGGER.warn("Creating local spark streaming context.");
            return getLocalSparkStreamingSession(jobConfig);
        } else {
            LOGGER.warn("Creating cluster spark streaming context");
            return createJavaStreamingContext(jobConfig);
        }
    }
}
