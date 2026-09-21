package com.example.temporal.model;

import java.math.BigDecimal;
import java.util.List;

// Inbound DTO: POST /orders request body and workflow input
public record OrderRequest(
    String orderId,
    String customerId,
    BigDecimal amount,
    List<String> items
) {}
