package com.jiangtao2545.kafkautils.exception;

public class KafkaOperationException extends RuntimeException {

    public KafkaOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
