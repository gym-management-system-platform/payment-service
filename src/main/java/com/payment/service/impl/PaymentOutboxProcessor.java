package com.payment.service.impl;


import com.payment.entity.OutboxEvent;
import com.payment.enums.OutboxEventType;
import com.payment.config.property.KafkaProperties;
import com.payment.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;


@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentOutboxProcessor {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaProperties kafkaProperties;

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms}")
    public void process() {
        outboxRepository.findTop50ByProcessedOrderByCreatedAtAsc(false)
                .flatMap(this::publishEvent)
                .subscribe();
    }

    private Mono<Void> publishEvent(OutboxEvent event) {
        try {
            if (event.getPayload() == null) {
                return markFailed(event, new IllegalStateException("Outbox payload is null"));
            }
            String payload = event.getPayload().asString();
            if (payload.isBlank()) {
                return markFailed(event, new IllegalStateException("Outbox payload string is empty"));
            }
            String topic = resolveTopic(event.getEventType());

            return Mono.fromFuture(
                            kafkaTemplate.send(topic, payload).toCompletableFuture()
                    )
                    .then(markProcessed(event))
                    .onErrorResume(e -> markFailed(event, e));
        } catch (Exception e) {
            return markFailed(event, e);
        }
    }

    private String resolveTopic(OutboxEventType eventType) {
        String eventKey = switch (eventType) {
            case PAYMENT_SUCCEEDED -> "payment-succeeded";
            case PAYMENT_FAILED -> "payment-failed";
        };

        String topic = kafkaProperties.getTopics().get(eventKey);
        if (topic == null || topic.isBlank()) {
            log.warn("Kafka topic for key '{}' is not configured, falling back to key name", eventKey);
            return eventKey;
        }
        return topic;
    }

    private Mono<Void> markProcessed(OutboxEvent event) {
        event.setProcessed(true);
        event.setProcessedAt(Instant.now());
        return outboxRepository.save(event).then();
    }

    private Mono<Void> markFailed(OutboxEvent event, Throwable error) {
        event.setLastError(error != null ? error.getMessage() : null);
        event.setRetryCount(event.getRetryCount() != null ? event.getRetryCount() + 1 : 1);
        return outboxRepository.save(event).then();
    }
}
