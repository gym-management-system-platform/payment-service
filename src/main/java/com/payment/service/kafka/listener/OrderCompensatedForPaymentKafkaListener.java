package com.payment.service.kafka.listener;

import com.payment.event.order.OrderCompensatedEvent;
import com.payment.service.kafka.PaymentOrderKafkaApplicationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderCompensatedForPaymentKafkaListener {

    private final PaymentOrderKafkaApplicationService paymentOrderKafka;

    @KafkaListener(
            topics = "${app.kafka.topics.order-compensated:order-compensated}",
            groupId = "payment-service-group",
            containerFactory = "orderCompensatedKafkaListenerContainerFactory"
    )
    public void handle(OrderCompensatedEvent event, Acknowledgment ack) {
        if (event == null) {
            ack.acknowledge();
            return;
        }
        paymentOrderKafka.onOrderCompensated(event)
                .doFinally(signalType -> ack.acknowledge())
                .subscribe(
                        v -> log.debug("Обработано order-compensated, sagaId={}", event.getSagaId()),
                        error -> log.error("Ошибка обработки order-compensated, sagaId={}", event.getSagaId(), error)
                );
    }
}
