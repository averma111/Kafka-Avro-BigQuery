package org.ashish.kafkaavro.factory.impl.processor;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.ashish.kafkaavro.common.JobConfig;
import org.ashish.kafkaavro.factory.iface.IJobProcessor;
import org.ashish.kafkaavro.factory.iface.IMessageConsumer;
import org.ashish.kafkaavro.model.MessageSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static org.ashish.kafkaavro.util.Constants.*;

public class DefaultJobProcessor extends IJobProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultJobProcessor.class.getName());
    private static final Long CHECK_INTERVAL = 180000L;
    private static boolean IS_JOB_FAILED = false;

    public DefaultJobProcessor(JobConfig jobConfig){
        super(jobConfig);
    }

    @Override
    public void process() throws Exception {

        try {
            Properties properties = this.jobConfig.getProperties();
            String jobInitiationTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            properties.put(JOB_INITIATION_TIME, jobInitiationTime);

            LOGGER.info("*********** Consume message from Source **********");
            IMessageConsumer messageConsumer =MessageConsumerFactory.getMessageConsumer(this.jobConfig,this.jobContext);
            JavaDStream<? extends MessageSchema> consumedDStream = messageConsumer.consume();


        }
    }
}
