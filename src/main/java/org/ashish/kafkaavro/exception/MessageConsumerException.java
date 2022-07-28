package org.ashish.kafkaavro.exception;

public class MessageConsumerException extends  Exception{

    public MessageConsumerException(String message) {
        super(message);

    }
    public MessageConsumerException(String message,Throwable cause){
        super(message,cause);
    }
}
