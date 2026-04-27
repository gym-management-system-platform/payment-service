package com.payment.service.impl;


import com.payment.config.property.PaymentMockProperties;
import com.payment.entity.Payment;
import com.payment.enums.Currency;
import com.payment.enums.PaymentMethod;
import com.payment.enums.PaymentStatus;
import com.payment.event.order.OrderProcessingPaymentEvent;
import com.payment.repository.PaymentRepository;
import com.payment.service.PaymentOutboxService;
import com.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;


@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentServiceImpl implements PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentOutboxService paymentOutboxService;
    private final PaymentMockProperties paymentMockProperties;
    private final TransactionalOperator transactionalOperator;

    @Override
    public Mono<Void> processPayment(OrderProcessingPaymentEvent event) {


        return paymentRepository.findBySagaId(event.getSagaId())
                .flatMap(existingPayment -> {
                    log.warn("Платёж уже существует для sagaId={}", event.getSagaId());
                    return Mono.<Void>empty();
                })
                .switchIfEmpty(
                        createPayment(event)
                                .flatMap(this::processPaymentTransaction)
                                .flatMap(this::publishPaymentResult)
                )
                .as(transactionalOperator::transactional)
                .then();
    }

    private Mono<Payment> createPayment(OrderProcessingPaymentEvent event) {
        Instant now = Instant.now();

        Payment payment = Payment.builder()
                .sagaId(event.getSagaId())
                .orderId(event.getOrderId())
                .amount(event.getAmount())
                .currency(event.getCurrency() != null ? event.getCurrency() : Currency.RUB)
                .status(PaymentStatus.PENDING)
                .paymentMethod(PaymentMethod.SBERBANK) //todo: перенести потом в заказ
                .createdAt(now)
                .updatedAt(now)
                .build();

        return paymentRepository.save(payment);
    }

    private Mono<Payment> processPaymentTransaction(Payment payment) {
        // Имитация обработки платежа
        return Mono.fromCallable(() -> {
            double p = paymentMockProperties.getMockSuccessProbability();
            boolean success = ThreadLocalRandom.current().nextDouble() < p;
            if (success) {
                payment.setStatus(PaymentStatus.SUCCEEDED);
                payment.setTransactionId("TXN-" + UUID.randomUUID());
                payment.setUpdatedAt(Instant.now());
            } else {
                payment.setStatus(PaymentStatus.FAILED);
                payment.setErrorMessage("Ошибка платёжного шлюза: недостаточно средств");
                payment.setUpdatedAt(Instant.now());
            }

            return payment;
        }).flatMap(paymentRepository::save);
    }

    private Mono<Void> publishPaymentResult(Payment payment) {
        if (PaymentStatus.SUCCEEDED.equals(payment.getStatus())) {
            return paymentOutboxService.enqueuePaymentSucceeded(payment);
        } else {
            return paymentOutboxService.enqueuePaymentFailed(payment);
        }
    }

    /**
     * Компенсирующая транзакция - возврат средств (refund)
     */
    @Override
    public Mono<Void> refundBySagaId(UUID sagaId) {
        return paymentRepository.findBySagaId(sagaId)
                .flatMap(payment -> {
                    if (!PaymentStatus.SUCCEEDED.equals(payment.getStatus())) {
                        log.warn("Возврат невозможен: статус платежа {} для sagaId={}", payment.getStatus(), sagaId);
                        return Mono.empty();
                    }

                    payment.setStatus(PaymentStatus.REFUNDED);
                    payment.setUpdatedAt(Instant.now());
                    return paymentRepository.save(payment);
                })
                .then()
                .as(transactionalOperator::transactional);
    }
}
