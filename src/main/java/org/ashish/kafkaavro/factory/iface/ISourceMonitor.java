package org.ashish.kafkaavro.factory.iface;

import org.ashish.kafkaavro.common.JobConfig;

public interface ISourceMonitor {

    Long getUnackedMessageInterval(JobConfig jobConfig,Long startTime, Long endTime);
}