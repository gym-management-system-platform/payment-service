package com.payment.event.base;


import lombok.Getter;
import lombok.experimental.SuperBuilder;

import java.time.Instant;
import java.util.UUID;


@Getter
@SuperBuilder
public abstract class BaseEvent {
    private final UUID sagaId;
    private final UUID orderId;
    private final Instant createdAt;
}
