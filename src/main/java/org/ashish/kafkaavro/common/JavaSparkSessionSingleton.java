package org.ashish.kafkaavro.common;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class JavaSparkSessionSingleton {

    private static SparkSession instance = null;

    private JavaSparkSessionSingleton()
    {

    }

    public static SparkSession getInstance(SparkConf sparkConf){
        if(instance == null){
            instance = SparkSession.builder()
                    .config(sparkConf)
                    .getOrCreate();
        }

        return instance;
    }
}
