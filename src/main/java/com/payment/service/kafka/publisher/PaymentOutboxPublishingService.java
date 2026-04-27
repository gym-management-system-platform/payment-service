package com.payment.service.kafka.publisher;

import com.payment.config.PaymentOutboxTopicResolver;
import com.payment.entity.OutboxEvent;
import com.payment.enums.OutboxEventStatus;
import com.payment.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class PaymentOutboxPublishingService {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final PaymentOutboxTopicResolver paymentOutboxTopicResolver;

    public Mono<Void> publishRow(OutboxEvent event) {
        try {
            if (event.getPayload() == null) {
                return markFailed(event, new IllegalStateException("Полезная нагрузка outbox отсутствует"));
            }
            String payload = event.getPayload().asString();
            if (payload.isBlank()) {
                return markFailed(event, new IllegalStateException("Полезная нагрузка outbox пуста"));
            }
            String topic = paymentOutboxTopicResolver.resolve(event.getEventType());
            return Mono.fromFuture(kafkaTemplate.send(topic, payload).toCompletableFuture())
                    .then(markProcessed(event))
                    .onErrorResume(e -> markFailed(event, e));
        } catch (Exception e) {
            return markFailed(event, e);
        }
    }

    private Mono<Void> markProcessed(OutboxEvent event) {
        event.setStatus(OutboxEventStatus.SENT);
        event.setProcessedAt(Instant.now());
        return outboxRepository.save(event).then();
    }

    private Mono<Void> markFailed(OutboxEvent event, Throwable error) {
        event.setStatus(OutboxEventStatus.FAILED);
        event.setLastError(error != null ? error.getMessage() : null);
        event.setRetryCount(event.getRetryCount() != null ? event.getRetryCount() + 1 : 1);
        return outboxRepository.save(event).then();
    }
}
