package com.payment.service.kafka.listener;


import com.payment.event.order.OrderProcessingPaymentEvent;
import com.payment.exception.PoisonMessageException;
import com.payment.service.impl.PaymentRetryAwareProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;


@Component
@RequiredArgsConstructor
@Slf4j
public class OrderProcessingPaymentKafkaListener {

    private final PaymentRetryAwareProcessingService processingService;

    @KafkaListener(
            topics = "${app.kafka.listener.payment.topic}",
            groupId = "${app.kafka.listener.payment.group-id}",
            containerFactory = "${app.kafka.listener.payment.container-factory}",
            concurrency = "${app.kafka.listener.payment.concurrency}"
    )
    @RetryableTopic(
            attempts = "${app.kafka.listener.payment.retry.attempts}",
            backoff = @Backoff(
                    delayExpression = "${app.kafka.listener.payment.retry.delay-ms}",
                    multiplierExpression = "${app.kafka.listener.payment.retry.multiplier}",
                    maxDelayExpression = "${app.kafka.listener.payment.retry.max-delay-ms}"
            ),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            exclude = {PoisonMessageException.class},
            autoCreateTopics = "${app.kafka.listener.payment.retry.auto-create-topics}",
            numPartitions = "${app.kafka.listener.payment.retry.num-partitions}",
            kafkaTemplate = "paymentRetryKafkaTemplate"
    )
    public void handle(OrderProcessingPaymentEvent event) {
        processingService.process(event);
    }

    @DltHandler
    public void handleDlt(OrderProcessingPaymentEvent event) {
        log.error(
                "Payment message sent to DLT, sagaId={}, orderId={}, amount={}",
                event.getSagaId(),
                event.getOrderId(),
                event.getAmount()
        );
    }
}
