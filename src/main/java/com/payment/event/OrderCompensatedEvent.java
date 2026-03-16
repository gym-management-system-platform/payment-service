package com.payment.event;


import java.time.Instant;


/**
 * Входящее событие для компенсации (возврат средств)
 */
public record OrderCompensatedEvent(
        String sagaId,
        String orderId,
        String reason,
        Instant createdAt
) {
}
