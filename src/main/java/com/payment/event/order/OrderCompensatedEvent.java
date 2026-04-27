package com.payment.event.order;


import lombok.Getter;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;


@Jacksonized
@Getter
@SuperBuilder
public class OrderCompensatedEvent extends OrderEvent {
    private final String reason;
}
