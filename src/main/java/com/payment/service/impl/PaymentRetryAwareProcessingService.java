package com.payment.service.impl;


import com.payment.config.property.PaymentMockProperties;
import com.payment.event.order.OrderProcessingPaymentEvent;
import com.payment.exception.PoisonMessageException;
import com.payment.exception.TransientPaymentException;
import com.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.concurrent.ThreadLocalRandom;


@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentRetryAwareProcessingService {

    private final PaymentService paymentService;
    private final PaymentMockProperties paymentMockProperties;

    public void process(OrderProcessingPaymentEvent event) {
        classifyOrSimulateFailure(event);
        paymentService.processPayment(event).block();
        log.info("Оплата прошла успешно, sagaId={}, amount={}", event.getSagaId(), event.getAmount());
    }

    private void classifyOrSimulateFailure(OrderProcessingPaymentEvent event) {
        BigDecimal amount = event.getAmount();

        // poison без симулятора: битый контракт → сразу DLT
        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new PoisonMessageException(
                    "Poison: невалидная сумма =" + amount + ", sagaId=" + event.getSagaId());
        }

        PaymentMockProperties.FailureSimulator simulator = paymentMockProperties.getFailureSimulator();
        if (simulator == null || !simulator.isEnabled()) {
            return;
        }

        if (amount.compareTo(BigDecimal.valueOf(simulator.getPoisonAmountThreshold())) > 0) {
            throw new PoisonMessageException(
                    "Poison: Сумма " + amount + " > threshold " + simulator.getPoisonAmountThreshold()
                            + ", sagaId=" + event.getSagaId());
        }

        maybeFailTransient(event, simulator);
    }

    private void maybeFailTransient(OrderProcessingPaymentEvent event,
                                    PaymentMockProperties.FailureSimulator simulator) {
        if (ThreadLocalRandom.current().nextDouble() < simulator.getTransientProbability()) {
            throw new TransientPaymentException(
                    "Симуляция критической ошибки sagaId=" + event.getSagaId());
        }
    }
}
