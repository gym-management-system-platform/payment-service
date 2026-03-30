package com.payment.event.base;

import lombok.Getter;
import lombok.experimental.SuperBuilder;

import java.time.Instant;

@Getter
@SuperBuilder
public abstract class BaseEvent {
    private final String sagaId;
    private final String orderId;
    private final Instant createdAt;
}
