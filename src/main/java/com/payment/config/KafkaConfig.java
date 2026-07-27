package com.payment.config;


import com.fitness.kafka.KafkaListenerContainerFactoryBuilder;
import com.fitness.kafka.KafkaProperties;
import com.payment.event.order.OrderCompensatedEvent;
import com.payment.event.order.OrderProcessingPaymentEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;


@Configuration
public class KafkaConfig {

    private final KafkaListenerContainerFactoryBuilder listenerFactoryBuilder;
    private final KafkaProperties kafkaProperties;

    public KafkaConfig(KafkaListenerContainerFactoryBuilder listenerFactoryBuilder, KafkaProperties kafkaProperties) {
        this.listenerFactoryBuilder = listenerFactoryBuilder;
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public PaymentOutboxTopicResolver paymentOutboxTopicResolver() {
        return new PaymentOutboxTopicResolver(kafkaProperties);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderProcessingPaymentEvent>
    orderProcessingPaymentKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderProcessingPaymentEvent> factory =
                listenerFactoryBuilder.json(OrderProcessingPaymentEvent.class);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setBatchListener(false);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderCompensatedEvent>
    orderCompensatedKafkaListenerContainerFactory() {
        return listenerFactoryBuilder.json(OrderCompensatedEvent.class);
    }

    @Bean
    public ProducerFactory<String, Object> paymentRetryProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        if (kafkaProperties.getProducer() != null) {
            props.put(ProducerConfig.RETRIES_CONFIG, kafkaProperties.getProducer().getRetries());
            props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, kafkaProperties.getProducer().getRetryBackoffMs());
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, kafkaProperties.getProducer().isIdempotence());
        }
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> paymentRetryKafkaTemplate(
            ProducerFactory<String, Object> paymentRetryProducerFactory) {
        return new KafkaTemplate<>(paymentRetryProducerFactory);
    }
}
