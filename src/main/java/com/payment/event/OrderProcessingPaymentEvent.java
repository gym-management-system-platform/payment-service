package com.payment.event;


import com.payment.enums.Currency;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Входящее событие от order-service для начала обработки платежа
 */
public record OrderProcessingPaymentEvent(
        String sagaId,
        String orderId,
        BigDecimal amount,
        Currency currency,
        Instant createdAt
) {
}
