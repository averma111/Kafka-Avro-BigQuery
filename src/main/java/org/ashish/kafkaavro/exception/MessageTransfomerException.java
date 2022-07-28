package org.ashish.kafkaavro.exception;

public class MessageTransfomerException extends  Exception{

        public MessageTransfomerException(String message) {
            super(message);

        }
        public MessageTransfomerException(String message,Throwable cause){
            super(message,cause);
        }
    }

