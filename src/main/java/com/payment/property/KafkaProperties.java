package com.payment.property;


import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;


@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaProperties {
    private String bootstrapServers;
    private Consumer consumer;
    private Producer producer;
    private Map<String, String> topics;
    private Listener listener;

    @Getter
    @Setter
    public static class Consumer {
        private String groupId;
        private int maxPollRecords;
        private int sessionTimeout;
        private int heartbeatInterval;
        private int maxPollIntervalMs;
    }

    @Getter
    @Setter
    public static class Producer {
        private boolean enable;
    }

    @Getter
    @Setter
    public static class Listener {
        private int pollTimeout;
    }
}