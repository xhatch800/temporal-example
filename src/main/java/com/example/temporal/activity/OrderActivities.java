package com.example.temporal.activity;

import com.example.temporal.model.OrderRequest;
import com.example.temporal.model.ValidatedOrder;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

import java.math.BigDecimal;

// @ActivityInterface marks this as a Temporal activity contract.
// Each method becomes a separately schedulable, retryable unit of work.
@ActivityInterface
public interface OrderActivities {

    // Validates the order fields; throws if invalid
    @ActivityMethod
    ValidatedOrder validateOrder(OrderRequest request);

    // Submits a payment charge; returns the transaction ID
    @ActivityMethod
    String processPayment(String orderId, BigDecimal amount);

    // Triggers fulfillment and shipping; returns a tracking number.
    // This is the long-running step — it reports heartbeat progress.
    @ActivityMethod
    String fulfillOrder(String orderId);

    // Sends an order confirmation; void because notification failures
    // shouldn't roll back a completed shipment
    @ActivityMethod
    void sendNotification(String orderId, String trackingNumber);
}
