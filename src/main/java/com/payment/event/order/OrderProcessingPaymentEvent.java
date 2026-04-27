package com.payment.event.order;


import com.payment.enums.Currency;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import java.math.BigDecimal;


@Jacksonized
@Getter
@SuperBuilder
public class OrderProcessingPaymentEvent extends OrderEvent {
    private final BigDecimal amount;
    private final Currency currency;
}
