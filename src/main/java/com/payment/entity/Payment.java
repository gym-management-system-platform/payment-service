package com.payment.entity;


import com.payment.enums.Currency;
import com.payment.enums.PaymentMethod;
import com.payment.enums.PaymentStatus;
import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;


@Table("payments")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Payment {

    @Id
    @Column("id")
    private UUID id;

    @Column("saga_id")
    private UUID sagaId;

    @Column("order_id")
    private UUID orderId;

    @Column("amount")
    private BigDecimal amount;

    @Column("currency")
    private Currency currency;

    @Column("status")
    private PaymentStatus status;

    @Column("payment_method")
    private PaymentMethod paymentMethod;

    @Column("transaction_id")
    private String transactionId;

    @Column("error_message")
    private String errorMessage;

    @Column("created_at")
    private Instant createdAt;

    @Column("updated_at")
    private Instant updatedAt;
}
