package com.payment.service;

import com.payment.entity.Payment;
import reactor.core.publisher.Mono;

/**
 * Запись платёжных исходящих событий в transactional outbox.
 */
public interface PaymentOutboxService {

    Mono<Void> enqueuePaymentSucceeded(Payment payment);

    Mono<Void> enqueuePaymentFailed(Payment payment);
}
