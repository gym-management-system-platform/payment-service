package com.payment.service.impl;


import com.payment.event.order.OrderCompensatedEvent;
import com.payment.event.order.OrderProcessingPaymentEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;


@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentSagaListener {

    private final PaymentServiceImpl paymentService;

    @KafkaListener(
            topics = "${app.kafka.topics.order-processing-payment:order-processing-payment}",
            groupId = "payment-service-group",
            containerFactory = "orderProcessingPaymentKafkaListenerContainerFactory"
    )
    public Mono<Void> handleOrderProcessingPayment(OrderProcessingPaymentEvent event) {
        if (event == null) {
            return Mono.empty();
        }

        return paymentService.processPayment(event)
                .doOnSuccess(v -> log.debug("Processed order-processing-payment"))
                .onErrorResume(ex -> {
                    log.error("Failed to process order-processing-payment: sagaId={}", event.getSagaId(), ex);
                    return Mono.empty();
                });
    }

    @KafkaListener(
            topics = "${app.kafka.topics.order-compensated:order-compensated}",
            groupId = "payment-service-group",
            containerFactory = "orderCompensatedKafkaListenerContainerFactory"
    )
    public Mono<Void> handleOrderCompensated(OrderCompensatedEvent event) {
        if (event == null) {
            return Mono.empty();
        }

        return paymentService.refundBySagaId(event.getSagaId())
                .doOnSuccess(v -> log.debug("Processed order-compensated for refund"))
                .onErrorResume(ex -> {
                    log.error("Failed to process order-compensated for refund: sagaId={}", event.getSagaId(), ex);
                    return Mono.empty();
                });
    }
}
