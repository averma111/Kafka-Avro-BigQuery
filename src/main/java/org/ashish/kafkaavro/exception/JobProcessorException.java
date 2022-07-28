package org.ashish.kafkaavro.exception;

public class JobProcessorException  extends Exception{
    public JobProcessorException(String message) {
        super(message);
    }
    public JobProcessorException(String message, Throwable cause){
        super(message,cause);
    }
}
