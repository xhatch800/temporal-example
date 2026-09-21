package com.example.temporal.workflow;

import com.example.temporal.activity.OrderActivities;
import com.example.temporal.model.OrderRequest;
import com.example.temporal.model.OrderResult;
import com.example.temporal.model.ValidatedOrder;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.workflow.Workflow;
import org.slf4j.Logger;

import java.time.Duration;

public class OrderWorkflowImpl implements OrderWorkflow {

    // Use Workflow.getLogger — it is replay-safe and won't produce duplicate log lines
    // when Temporal re-executes workflow history during recovery.
    private static final Logger log = Workflow.getLogger(OrderWorkflowImpl.class);

    // Each activity gets its own stub carrying tailored ActivityOptions.
    // Multiple stubs for the same @ActivityInterface are valid — each stub is
    // just a typed proxy; the options travel with the stub, not the interface.

    private final OrderActivities validateActivities = Workflow.newActivityStub(
        OrderActivities.class,
        ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(5))
            .setRetryOptions(RetryOptions.newBuilder()
                .setMaximumAttempts(3)
                .setInitialInterval(Duration.ofSeconds(1))
                .setBackoffCoefficient(2.0)
                .build())
            .build());

    private final OrderActivities paymentActivities = Workflow.newActivityStub(
        OrderActivities.class,
        ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(10))
            .setRetryOptions(RetryOptions.newBuilder()
                .setMaximumAttempts(5)        // payment gets more retries — transient failures are common
                .setInitialInterval(Duration.ofSeconds(2))
                .setBackoffCoefficient(2.0)
                .build())
            .build());

    private final OrderActivities fulfillActivities = Workflow.newActivityStub(
        OrderActivities.class,
        ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(30))
            // heartbeatTimeout: if the activity doesn't call heartbeat() within this window,
            // Temporal treats the worker as dead and reschedules the activity.
            .setHeartbeatTimeout(Duration.ofSeconds(10))
            .setRetryOptions(RetryOptions.newBuilder()
                .setMaximumAttempts(3)
                .setInitialInterval(Duration.ofSeconds(5))
                .setBackoffCoefficient(2.0)
                .build())
            .build());

    private final OrderActivities notifyActivities = Workflow.newActivityStub(
        OrderActivities.class,
        ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(10))
            .setRetryOptions(RetryOptions.newBuilder()
                .setMaximumAttempts(3)
                .setInitialInterval(Duration.ofSeconds(1))
                .setBackoffCoefficient(2.0)
                .build())
            .build());

    @Override
    public OrderResult processOrder(OrderRequest request) {
        log.info("Starting order workflow for orderId={}", request.orderId());

        try {
            // Step 1: Validate — fast, low retry budget
            ValidatedOrder validated = validateActivities.validateOrder(request);
            log.info("Validated orderId={}", validated.orderId());

            // Step 2: Payment — more retries to handle transient payment-gateway hiccups
            String transactionId = paymentActivities.processPayment(request.orderId(), request.amount());
            log.info("Payment accepted orderId={} txnId={}", request.orderId(), transactionId);

            // Step 3: Fulfillment — long-running, heartbeats to report progress
            String trackingNumber = fulfillActivities.fulfillOrder(request.orderId());
            log.info("Fulfilled orderId={} tracking={}", request.orderId(), trackingNumber);

            // Step 4: Notify — fire-and-forget; failure doesn't roll back shipment
            notifyActivities.sendNotification(request.orderId(), trackingNumber);
            log.info("Notification sent for orderId={}", request.orderId());

            return OrderResult.success(request.orderId(), trackingNumber, transactionId);

        } catch (ActivityFailure e) {
            // ActivityFailure wraps the real cause (ApplicationFailure, TimeoutFailure, etc.).
            // We catch here to return a structured failure result instead of failing the workflow,
            // which allows callers to query the final status.
            String cause = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
            log.error("Order failed orderId={} cause={}", request.orderId(), cause);
            return OrderResult.failure(request.orderId(), cause);
        }
    }
}
