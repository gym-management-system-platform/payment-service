package com.payment.entity;


import com.payment.enums.OutboxAggregateType;
import com.payment.enums.OutboxEventType;
import lombok.*;
import io.r2dbc.postgresql.codec.Json;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;


@Table("outbox_event")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEvent {
    @Id
    @Column("id")
    private UUID id;

    @Column("aggregate_type")
    private OutboxAggregateType aggregateType;

    @Column("aggregate_id")
    private String aggregateId;

    @Column("event_type")
    private OutboxEventType eventType;

    @Column("payload")
    private Json payload;

    @Column("saga_id")
    private String sagaId;

    @Column("created_at")
    private Instant createdAt;

    @Column("processed")
    private Boolean processed;

    @Column("processed_at")
    private Instant processedAt;

    @Column("retry_count")
    private Integer retryCount;

    @Column("last_error")
    private String lastError;
}
