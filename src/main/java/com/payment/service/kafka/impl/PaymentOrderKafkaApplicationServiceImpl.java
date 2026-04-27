package com.payment.service.kafka.impl;

import com.payment.event.order.OrderCompensatedEvent;
import com.payment.event.order.OrderProcessingPaymentEvent;
import com.payment.service.PaymentService;
import com.payment.service.kafka.PaymentOrderKafkaApplicationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentOrderKafkaApplicationServiceImpl implements PaymentOrderKafkaApplicationService {

    private final PaymentService paymentService;

    @Override
    public Mono<Void> onOrderProcessingPayment(OrderProcessingPaymentEvent event) {
        return paymentService.processPayment(event)
                .doOnSuccess(v -> log.debug("Обработано order-processing-payment"))
                .onErrorResume(ex -> {
                    log.error("Ошибка обработки order-processing-payment, sagaId={}", event.getSagaId(), ex);
                    return Mono.empty();
                });
    }

    @Override
    public Mono<Void> onOrderCompensated(OrderCompensatedEvent event) {
        return paymentService.refundBySagaId(event.getSagaId())
                .doOnSuccess(v -> log.debug("Обработано order-compensated (возврат)"))
                .onErrorResume(ex -> {
                    log.error("Ошибка обработки order-compensated для возврата, sagaId={}", event.getSagaId(), ex);
                    return Mono.empty();
                });
    }
}
