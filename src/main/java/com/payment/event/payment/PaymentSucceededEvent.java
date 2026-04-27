package com.payment.event.payment;

import lombok.Getter;
import lombok.experimental.SuperBuilder;

import java.util.UUID;


@Getter
@SuperBuilder
public class PaymentSucceededEvent extends PaymentEvent {
    private final UUID paymentId;
}
