package com.payment.service;


import com.payment.event.order.OrderProcessingPaymentEvent;
import reactor.core.publisher.Mono;

import java.util.UUID;


public interface PaymentService {
    /**
     * Обработка платежа
     */
    Mono<Void> processPayment(OrderProcessingPaymentEvent event);

    /**
     * Компенсирующая транзакция
     */
    Mono<Void> refundBySagaId(UUID sagaId);
}
