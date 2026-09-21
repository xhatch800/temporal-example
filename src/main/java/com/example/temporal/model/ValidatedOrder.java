package com.example.temporal.model;

import java.math.BigDecimal;
import java.util.List;

// Returned by validateOrder to confirm the order passed validation
public record ValidatedOrder(
    String orderId,
    String customerId,
    BigDecimal amount,
    List<String> items
) {}
