package org.ashish.kafkaavro.model;

import java.io.Serializable;

public class MessageSchema implements Serializable {

    protected String tableName;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
