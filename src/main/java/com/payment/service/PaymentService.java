package com.payment.service;


import com.payment.entity.OutboxEvent;
import com.payment.entity.Payment;
import com.payment.enums.Currency;
import com.payment.enums.OutboxAggregateType;
import com.payment.enums.OutboxEventType;
import com.payment.enums.PaymentStatus;
import com.payment.event.OrderProcessingPaymentEvent;
import com.payment.event.PaymentFailedEvent;
import com.payment.event.PaymentSucceededEvent;
import com.payment.repository.OutboxRepository;
import com.payment.repository.PaymentRepository;
import com.payment.serialize.EventSerializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;


@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final OutboxRepository outboxRepository;
    private final EventSerializer serializer;
    private final TransactionalOperator transactionalOperator;

    public Mono<Void> processPayment(OrderProcessingPaymentEvent event) {
        String sagaId = event.sagaId();
        String orderId = event.orderId();

        return paymentRepository.findBySagaId(sagaId)
                .flatMap(existingPayment -> {
                    log.warn("Payment already exists for sagaId: {}", sagaId);
                    return Mono.<Void>empty();
                })
                .switchIfEmpty(
                        createPayment(event)
                                .flatMap(payment -> processPaymentTransaction(payment, event))
                                .flatMap(this::publishPaymentResult)
                )
                .as(transactionalOperator::transactional)
                .then();
    }

    private Mono<Payment> createPayment(OrderProcessingPaymentEvent event) {
        Payment payment = Payment.builder()
                .id(UUID.randomUUID())
                .sagaId(event.sagaId())
                .orderId(event.orderId())
                .amount(event.amount())
                .currency(event.currency() != null ? event.currency() : Currency.RUB)
                .status(PaymentStatus.PENDING)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();

        return paymentRepository.save(payment);
    }

    private Mono<Payment> processPaymentTransaction(Payment payment, OrderProcessingPaymentEvent event) {
        // Имитация обработки платежа
        return Mono.fromCallable(() -> {
            // 90% успешных платежей для демонстрации
            boolean success = Math.random() < 0.9;
            if (success) {
                payment.setStatus(PaymentStatus.SUCCEEDED);
                payment.setTransactionId("TXN-" + UUID.randomUUID());
                payment.setUpdatedAt(Instant.now());
            } else {
                payment.setStatus(PaymentStatus.FAILED);
                payment.setErrorMessage("Payment gateway error: insufficient funds");
                payment.setUpdatedAt(Instant.now());
            }

            return payment;
        }).flatMap(paymentRepository::save);
    }

    private Mono<Void> publishPaymentResult(Payment payment) {
        if (PaymentStatus.SUCCEEDED.equals(payment.getStatus())) {
            return publishPaymentSucceeded(payment);
        } else {
            return publishPaymentFailed(payment);
        }
    }

    private Mono<Void> publishPaymentSucceeded(Payment payment) {
        PaymentSucceededEvent event = new PaymentSucceededEvent(
                payment.getSagaId(),
                payment.getOrderId(),
                payment.getId().toString(),
                Instant.now()
        );


        OutboxEvent outbox = OutboxEvent.builder()
                .id(UUID.randomUUID())
                .aggregateType(OutboxAggregateType.PAYMENT)
                .aggregateId(payment.getId().toString())
                .eventType(OutboxEventType.PAYMENT_SUCCEEDED)
                .payload(serializer.toJson(event))
                .sagaId(payment.getSagaId())
                .createdAt(Instant.now())
                .processed(false)
                .retryCount(0)
                .build();

        return outboxRepository.save(outbox).then();
    }

    private Mono<Void> publishPaymentFailed(Payment payment) {
        PaymentFailedEvent event = new PaymentFailedEvent(
                payment.getSagaId(),
                payment.getOrderId(),
                payment.getErrorMessage(),
                Instant.now());

        OutboxEvent outbox = OutboxEvent.builder()
                .id(UUID.randomUUID())
                .aggregateType(OutboxAggregateType.PAYMENT)
                .aggregateId(payment.getId().toString())
                .eventType(OutboxEventType.PAYMENT_FAILED)
                .payload(serializer.toJson(event))
                .sagaId(payment.getSagaId())
                .createdAt(Instant.now())
                .processed(false)
                .retryCount(0)
                .build();

        return outboxRepository.save(outbox).then();
    }

    /**
     * Компенсирующая транзакция - возврат средств (refund)
     */
    public Mono<Void> refundBySagaId(String sagaId) {
        return paymentRepository.findBySagaId(sagaId)
                .flatMap(payment -> {
                    if (!PaymentStatus.SUCCEEDED.equals(payment.getStatus())) {
                        log.warn("Cannot refund payment with status: {} for sagaId: {}", payment.getStatus(), sagaId);
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
