package com.payment.enums;


public enum PaymentStatus {
    CREATED,     // Создан
    PENDING,     // Ожидает подтверждения
    SUCCEEDED,   // Успешно завершен
    FAILED,      // Неуспешный платеж
    REFUNDED,    // Полностью возвращен
    PARTIALLY_REFUNDED // Частично возвращен
}
