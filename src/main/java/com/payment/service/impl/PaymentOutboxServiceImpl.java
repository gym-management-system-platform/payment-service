package com.payment.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.payment.entity.OutboxEvent;
import com.payment.entity.Payment;
import com.payment.enums.OutboxAggregateType;
import com.payment.enums.OutboxEventStatus;
import com.payment.enums.OutboxEventType;
import com.payment.event.payment.PaymentFailedEvent;
import com.payment.event.payment.PaymentSucceededEvent;
import com.payment.repository.OutboxRepository;
import com.payment.service.PaymentOutboxService;
import io.r2dbc.postgresql.codec.Json;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class PaymentOutboxServiceImpl implements PaymentOutboxService {

    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Override
    public Mono<Void> enqueuePaymentSucceeded(Payment payment) {
        PaymentSucceededEvent event = PaymentSucceededEvent.builder()
                .sagaId(payment.getSagaId())
                .orderId(payment.getOrderId())
                .paymentId(payment.getId())
                .createdAt(Instant.now())
                .build();

        OutboxEvent outbox = OutboxEvent.builder()
                .aggregateType(OutboxAggregateType.PAYMENT)
                .aggregateId(payment.getId())
                .eventType(OutboxEventType.PAYMENT_SUCCEEDED)
                .payload(toJson(event))
                .sagaId(payment.getSagaId())
                .createdAt(Instant.now())
                .status(OutboxEventStatus.NEW)
                .retryCount(0)
                .build();
        return outboxRepository.save(outbox).then();
    }

    @Override
    public Mono<Void> enqueuePaymentFailed(Payment payment) {
        PaymentFailedEvent event = PaymentFailedEvent.builder()
                .sagaId(payment.getSagaId())
                .orderId(payment.getOrderId())
                .reason(payment.getErrorMessage())
                .createdAt(Instant.now())
                .build();

        OutboxEvent outbox = OutboxEvent.builder()
                .aggregateType(OutboxAggregateType.PAYMENT)
                .aggregateId(payment.getId())
                .eventType(OutboxEventType.PAYMENT_FAILED)
                .payload(toJson(event))
                .sagaId(payment.getSagaId())
                .createdAt(Instant.now())
                .status(OutboxEventStatus.NEW)
                .retryCount(0)
                .build();
        return outboxRepository.save(outbox).then();
    }

    private Json toJson(Object event) {
        try {
            return Json.of(objectMapper.writeValueAsString(event));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Не удалось сериализовать событие для outbox", e);
        }
    }
}
