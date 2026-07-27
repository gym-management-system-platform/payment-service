package com.payment.exception;


public class PoisonMessageException extends RuntimeException {
    public PoisonMessageException(String message) {
        super(message);
    }
}
