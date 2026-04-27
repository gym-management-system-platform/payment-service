package com.payment.service.kafka.publisher;

import com.payment.enums.OutboxEventStatus;
import com.payment.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PaymentOutboxKafkaScheduler {

    private final OutboxRepository outboxRepository;
    private final PaymentOutboxPublishingService paymentOutboxPublishingService;

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms}")
    public void pollAndPublish() {
        outboxRepository.findTop50ByStatusOrderByCreatedAtAsc(OutboxEventStatus.NEW)
                .flatMap(paymentOutboxPublishingService::publishRow)
                .subscribe();
    }
}
