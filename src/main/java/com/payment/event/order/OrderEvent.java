package com.payment.event.order;

import com.payment.event.base.BaseEvent;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
public abstract class OrderEvent extends BaseEvent {
}
