package org.ashish.kafkaavro.common;

import java.io.Serializable;
import java.util.Properties;

public class JobConfig implements Serializable {

    private Properties properties;

    public JobConfig(Properties properties) {
        this.properties =properties;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

}
