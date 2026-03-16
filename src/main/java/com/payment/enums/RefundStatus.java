package com.payment.enums;


public enum RefundStatus {
    CREATED,     // Создан
    PROCESSING,  // В обработке
    SUCCEEDED,   // Успешно возвращен
    FAILED       // Не удалось вернуть
}
