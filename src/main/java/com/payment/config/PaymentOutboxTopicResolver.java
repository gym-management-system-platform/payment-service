package com.payment.config;

import com.fitness.kafka.KafkaProperties;
import com.payment.enums.OutboxEventType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@RequiredArgsConstructor
@Slf4j
public class PaymentOutboxTopicResolver {

    private final KafkaProperties kafkaProperties;

    public String resolve(OutboxEventType eventType) {
        String key = eventType.topicConfigKey();
        Map<String, String> topics = kafkaProperties.getTopics();
        if (topics == null) {
            log.warn("app.kafka.topics не задан, в качестве имени топика используется ключ '{}'", key);
            return key;
        }
        String topic = topics.get(key);
        if (topic == null || topic.isBlank()) {
            log.warn("Топик Kafka для ключа '{}' не задан в конфигурации, используется сам ключ как имя топика", key);
            return key;
        }
        return topic;
    }
}
