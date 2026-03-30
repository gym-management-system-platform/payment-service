package com.payment.event.order;

import lombok.Getter;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
public class OrderCompensatedEvent extends OrderEvent {
    private final String reason;
}
