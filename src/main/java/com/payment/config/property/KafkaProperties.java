package com.payment.config.property;


import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Map;


@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaProperties {

    private String bootstrapServers;

    @NestedConfigurationProperty
    private Deserializer deserializer;

    @NestedConfigurationProperty
    private Consumer consumer;

    @NestedConfigurationProperty
    private Producer producer;

    @NestedConfigurationProperty
    private Listener listener;

    @NestedConfigurationProperty
    private Error error;

    private Map<String, String> topics;

    @Getter
    @Setter
    public static class Deserializer {
        String trustedPackages;
        Boolean useTypeInfoHeaders;
    }

    @Getter
    @Setter
    public static class Consumer {
        private String groupId;
        private int maxPollRecords;
        private int sessionTimeout;
        private int heartbeatInterval;
        private int maxPollIntervalMs;
        private boolean enableAutoCommit;
        private String autoOffsetReset;
    }

    @Getter
    @Setter
    public static class Producer {
        private boolean enable;
        private int retries;
        private int retryBackoffMs;
        private boolean idempotence;
    }

    @Getter
    @Setter
    public static class Listener {
        private boolean batchListener;
        private int pollTimeout;
        private int concurrency;
        private boolean micrometerEnabled;
    }

    @Getter
    @Setter
    public static class Error {

        @NestedConfigurationProperty
        private Retry retry;

        @Getter
        @Setter
        public static class Retry {
            private int backoffMs;
            private int maxAttempts;
        }
    }
}
