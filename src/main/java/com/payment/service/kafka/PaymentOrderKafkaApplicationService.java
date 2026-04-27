package com.payment.service.kafka;

import com.payment.event.order.OrderCompensatedEvent;
import com.payment.event.order.OrderProcessingPaymentEvent;
import reactor.core.publisher.Mono;

public interface PaymentOrderKafkaApplicationService {

    Mono<Void> onOrderProcessingPayment(OrderProcessingPaymentEvent event);

    Mono<Void> onOrderCompensated(OrderCompensatedEvent event);
}
