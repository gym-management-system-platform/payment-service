package com.payment.event;


import java.time.Instant;

/**
 * Исходящее событие - платеж не прошел
 */
public record PaymentFailedEvent(
        String sagaId,
        String orderId,
        String reason,
        Instant createdAt
) {
}
