package com.payment.config;


import com.fitness.kafka.KafkaListenerContainerFactoryBuilder;
import com.fitness.kafka.KafkaProperties;
import com.payment.event.order.OrderCompensatedEvent;
import com.payment.event.order.OrderProcessingPaymentEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;


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
    public ConcurrentKafkaListenerContainerFactory<String, OrderProcessingPaymentEvent> orderProcessingPaymentKafkaListenerContainerFactory() {
        return listenerFactoryBuilder.json(OrderProcessingPaymentEvent.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderCompensatedEvent> orderCompensatedKafkaListenerContainerFactory() {
        return listenerFactoryBuilder.json(OrderCompensatedEvent.class);
    }
}