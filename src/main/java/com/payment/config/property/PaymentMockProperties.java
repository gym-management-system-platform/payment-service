package com.payment.config.property;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Параметры имитации платежного шлюза.
 */
@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "app.payment")
public class PaymentMockProperties {

    /**
     * Вероятность успешного платежа в диапазоне [0.0, 1.0].
     */
    private double mockSuccessProbability;
}
