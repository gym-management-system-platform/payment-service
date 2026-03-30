package com.payment.config;

import com.payment.config.property.KafkaProperties;
import com.payment.event.order.OrderCompensatedEvent;
import com.payment.event.order.OrderProcessingPaymentEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;

    public KafkaConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.RETRIES_CONFIG, kafkaProperties.getProducer().getRetries());
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, kafkaProperties.getProducer().getRetryBackoffMs());
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, kafkaProperties.getProducer().isIdempotence());
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderProcessingPaymentEvent> orderProcessingPaymentKafkaListenerContainerFactory() {
        return typedListenerFactory(OrderProcessingPaymentEvent.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderCompensatedEvent> orderCompensatedKafkaListenerContainerFactory() {
        return typedListenerFactory(OrderCompensatedEvent.class);
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> typedListenerFactory(Class<T> targetType) {
        ConcurrentKafkaListenerContainerFactory<String, T> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(jsonConsumerFactory(targetType));
        factory.setBatchListener(kafkaProperties.getListener().isBatchListener());
        factory.setConcurrency(kafkaProperties.getListener().getConcurrency());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setPollTimeout(kafkaProperties.getListener().getPollTimeout());
        factory.getContainerProperties().setMicrometerEnabled(kafkaProperties.getListener().isMicrometerEnabled());
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

    private <T> ConsumerFactory<String, T> jsonConsumerFactory(Class<T> targetType) {
        Map<String, Object> props = baseConsumerProps();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, targetType);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, kafkaProperties.getDeserializer().getTrustedPackages());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, kafkaProperties.getDeserializer().getUseTypeInfoHeaders());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    private Map<String, Object> baseConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getConsumer().getGroupId());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, kafkaProperties.getConsumer().getSessionTimeout());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaProperties.getConsumer().getMaxPollRecords());
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, kafkaProperties.getConsumer().getMaxPollIntervalMs());
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, kafkaProperties.getConsumer().getHeartbeatInterval());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaProperties.getConsumer().isEnableAutoCommit());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaProperties.getConsumer().getAutoOffsetReset());
        return props;
    }

    private CommonErrorHandler errorHandler() {
        KafkaProperties.Error.Retry retry = kafkaProperties.getError().getRetry();
        DefaultErrorHandler handler = new DefaultErrorHandler(
                new FixedBackOff(retry.getBackoffMs(), retry.getMaxAttempts()));

        handler.addNotRetryableExceptions(IllegalStateException.class);
        handler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.info("RetryListeners message = {}, offset = {} deliveryAttempt = {}",
                        ex.getMessage(), record.offset(), deliveryAttempt));
        return handler;
    }
}