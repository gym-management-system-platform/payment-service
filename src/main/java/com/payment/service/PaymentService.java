package com.payment.service;


import com.payment.event.order.OrderProcessingPaymentEvent;
import reactor.core.publisher.Mono;


public interface PaymentService {
    /**
     * Обработка платежа (основной сценарий Saga)
     */
    Mono<Void> processPayment(OrderProcessingPaymentEvent event);

    /**
     * Компенсирующая транзакция (refund)
     */
    Mono<Void> refundBySagaId(String sagaId);
}
