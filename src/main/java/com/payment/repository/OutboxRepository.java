package com.payment.repository;


import com.payment.entity.OutboxEvent;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

import java.util.UUID;


@Repository
public interface OutboxRepository extends R2dbcRepository<OutboxEvent, UUID> {
    Flux<OutboxEvent> findTop50ByProcessedOrderByCreatedAtAsc(boolean processed);
}
