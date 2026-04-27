package com.payment.repository;


import com.payment.entity.Payment;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.UUID;


@Repository
public interface PaymentRepository extends R2dbcRepository<Payment, UUID> {

    Mono<Payment> findBySagaId(UUID sagaId);

}
