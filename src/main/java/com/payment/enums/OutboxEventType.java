package com.payment.enums;


public enum OutboxEventType {
    PAYMENT_SUCCEEDED("payment-succeeded"),
    PAYMENT_FAILED("payment-failed");

    private final String topicConfigKey;

    OutboxEventType(String topicConfigKey) {
        this.topicConfigKey = topicConfigKey;
    }

    public String topicConfigKey() {
        return topicConfigKey;
    }
}
