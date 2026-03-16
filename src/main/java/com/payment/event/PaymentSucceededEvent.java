package com.payment.event;


import java.time.Instant;


/**
 * Исходящее событие - платеж успешен
 */
public record PaymentSucceededEvent(
        String sagaId,
        String orderId,
        String paymentId,
        Instant createdAt
) {
}
