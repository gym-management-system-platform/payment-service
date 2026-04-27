package com.payment.event.payment;


import lombok.Getter;
import lombok.experimental.SuperBuilder;


@Getter
@SuperBuilder
public class PaymentFailedEvent extends PaymentEvent {
    private final String reason;
}
