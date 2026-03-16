package com.payment.service;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.payment.event.OrderCompensatedEvent;
import com.payment.event.OrderProcessingPaymentEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;


@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentSagaListener {

    private final PaymentService paymentService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${app.kafka.topics.order-processing-payment:order-processing-payment}",
            groupId = "payment-service-group"
    )
    public Mono<Void> handleOrderProcessingPayment(String message) {
        if (message == null || message.isBlank()) {
            return Mono.empty();
        }

        return parseOrderProcessingPayment(message)
                .flatMap(paymentService::processPayment)
                .doOnSuccess(v -> log.debug("Processed order-processing-payment"))
                .onErrorResume(ex -> {
                    log.error("Failed to process order-processing-payment: {}", message, ex);
                    return Mono.empty();
                });
    }

    @KafkaListener(
            topics = "${app.kafka.topics.order-compensated:order-compensated}",
            groupId = "payment-service-group"
    )
    public Mono<Void> handleOrderCompensated(String message) {
        if (message == null || message.isBlank()) {
            return Mono.empty();
        }

        return parseOrderCompensated(message)
                .flatMap(event -> paymentService.refundBySagaId(event.sagaId()))
                .doOnSuccess(v -> log.debug("Processed order-compensated for refund"))
                .onErrorResume(ex -> {
                    log.error("Failed to process order-compensated for refund: {}", message, ex);
                    return Mono.empty();
                });
    }

    private Mono<OrderProcessingPaymentEvent> parseOrderProcessingPayment(String message) {
        return Mono.fromCallable(() -> objectMapper.readValue(message, OrderProcessingPaymentEvent.class))
                .onErrorResume(e -> {
                    log.warn("Could not parse order-processing-payment: {}", message, e);
                    return Mono.empty();
                });
    }

    private Mono<OrderCompensatedEvent> parseOrderCompensated(String message) {
        return Mono.fromCallable(() -> objectMapper.readValue(message, OrderCompensatedEvent.class))
                .onErrorResume(e -> {
                    log.warn("Could not parse order-compensated: {}", message, e);
                    return Mono.empty();
                });
    }
}
