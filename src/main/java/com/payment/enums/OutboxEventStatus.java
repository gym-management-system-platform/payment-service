package com.payment.enums;

/**
 * Статус доставки записи {@code outbox_event} в Kafka (аналогично order-service).
 */
public enum OutboxEventStatus {
    /** Ожидает публикации. */
    NEW,
    /** Успешно отправлено в брокер. */
    SENT,
    /** Ошибка публикации (см. {@code last_error}, счётчик повторов). */
    FAILED
}
