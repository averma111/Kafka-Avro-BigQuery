package org.ashish.kafkaavro.exception;

public class MessageWriterException extends Exception{

    public MessageWriterException(String message){
        super(message);
    }
    public MessageWriterException(String message, Throwable cause){
        super(message, cause);
    }
}
