package org.ashish.kafkaavro.model;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class TransformData {

    private Dataset<Row> dataToWriteDs = null;
    private Dataset<Row> errorLogDs = null;

    public TransformData(Dataset<Row> dataToWriteDs, Dataset<Row> errorLogDs) {
        this.dataToWriteDs = dataToWriteDs;
        this.errorLogDs = errorLogDs;
    }

    public Dataset<Row> getDataToWriteDs() {
        return dataToWriteDs;
    }

    public void setDataToWriteDs(Dataset<Row> dataToWriteDs) {
        this.dataToWriteDs = dataToWriteDs;
    }

    public Dataset<Row> getErrorLogDs() {
        return errorLogDs;
    }

    public void setErrorLogDs(Dataset<Row> errorLogDs) {
        this.errorLogDs = errorLogDs;
    }
}
