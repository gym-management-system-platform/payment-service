package com.payment.event.payment;

import lombok.Getter;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
public class PaymentSucceededEvent extends PaymentEvent {
    private final String paymentId;
}
