package com.framework.broker.exception;

public class BackpressureException extends BrokerException {
    public BackpressureException(String message) {
        super(message);
    }
}