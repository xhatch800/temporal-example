package com.example.temporal.model;

// The workflow's return value — carry status so the workflow never throws on business failures
public record OrderResult(
    String orderId,
    String status,          // "SUCCESS" or "FAILED"
    String trackingNumber,  // populated on success
    String transactionId,   // populated on success
    String errorMessage     // populated on failure
) {
    public static OrderResult success(String orderId, String trackingNumber, String transactionId) {
        return new OrderResult(orderId, "SUCCESS", trackingNumber, transactionId, null);
    }

    public static OrderResult failure(String orderId, String errorMessage) {
        return new OrderResult(orderId, "FAILED", null, null, errorMessage);
    }
}
