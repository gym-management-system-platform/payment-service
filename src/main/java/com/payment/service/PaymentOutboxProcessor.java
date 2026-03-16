package com.payment.service;


import com.payment.entity.OutboxEvent;
import com.payment.enums.OutboxEventType;
import com.payment.event.PaymentFailedEvent;
import com.payment.event.PaymentSucceededEvent;
import com.payment.property.KafkaProperties;
import com.payment.repository.OutboxRepository;
import com.payment.serialize.EventSerializer;
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
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final EventSerializer serializer;
    private final KafkaProperties kafkaProperties;

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms}")
    public void process() {
        outboxRepository.findTop50ByProcessedOrderByCreatedAtAsc(false)
                .flatMap(this::publishEvent)
                .subscribe();
    }

    private Mono<Void> publishEvent(OutboxEvent event) {
        try {
            Object payload = deserialize(event);
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

    private Object deserialize(OutboxEvent event) {
        OutboxEventType type = event.getEventType();
        if (OutboxEventType.PAYMENT_SUCCEEDED.equals(type)) {
            return serializer.fromJson(event.getPayload(), PaymentSucceededEvent.class);
        }
        if (OutboxEventType.PAYMENT_FAILED.equals(type)) {
            return serializer.fromJson(event.getPayload(), PaymentFailedEvent.class);
        }
        throw new IllegalArgumentException("Unknown event type: " + type);
    }

    private String resolveTopic(OutboxEventType eventType) {
        String eventKey = eventType != null ? eventType.toString() : "unknown";

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
