package com.payment.service.kafka.listener;

import com.payment.event.order.OrderProcessingPaymentEvent;
import com.payment.service.kafka.PaymentOrderKafkaApplicationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderProcessingPaymentKafkaListener {

    private final PaymentOrderKafkaApplicationService paymentOrderKafka;

    @KafkaListener(
            topics = "${app.kafka.topics.order-processing-payment:order-processing-payment}",
            groupId = "payment-service-group",
            containerFactory = "orderProcessingPaymentKafkaListenerContainerFactory"
    )
    public void handle(OrderProcessingPaymentEvent event, Acknowledgment ack) {
        if (event == null) {
            ack.acknowledge();
            return;
        }
        paymentOrderKafka.onOrderProcessingPayment(event)
                .doFinally(signalType -> ack.acknowledge())
                .subscribe(
                        v -> log.debug("Обработано order-processing-payment, sagaId={}", event.getSagaId()),
                        error -> log.error("Ошибка обработки order-processing-payment, sagaId={}", event.getSagaId(), error)
                );
    }
}
