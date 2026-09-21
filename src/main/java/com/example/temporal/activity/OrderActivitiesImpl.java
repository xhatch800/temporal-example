package com.example.temporal.activity;

import com.example.temporal.model.OrderRequest;
import com.example.temporal.model.ValidatedOrder;
import io.temporal.activity.Activity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

// @Service makes this a Spring bean — activities can use @Value, @Autowired, etc.
// The worker receives this instance via registerActivitiesImplementations().
@Service
public class OrderActivitiesImpl implements OrderActivities {

    private static final Logger log = LoggerFactory.getLogger(OrderActivitiesImpl.class);

    // When true, processPayment throws on the first two calls to demonstrate Temporal
    // retrying the activity. Toggle via application.yml or --temporal.activities.simulate-payment-failure=true
    @Value("${temporal.activities.simulate-payment-failure:false}")
    private boolean simulatePaymentFailure;

    // Tracks invocations to produce a controlled failure pattern when simulation is on.
    // AtomicInteger is safe here because each Temporal activity attempt runs on one thread.
    private final AtomicInteger paymentCallCount = new AtomicInteger(0);

    @Override
    public ValidatedOrder validateOrder(OrderRequest request) {
        log.info("Validating order orderId={}", request.orderId());

        if (request.orderId() == null || request.orderId().isBlank()) {
            throw new IllegalArgumentException("orderId must not be blank");
        }
        if (request.amount() == null || request.amount().compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("amount must be positive");
        }
        if (request.items() == null || request.items().isEmpty()) {
            throw new IllegalArgumentException("order must contain at least one item");
        }

        log.info("Order validated orderId={}", request.orderId());
        return new ValidatedOrder(
            request.orderId(),
            request.customerId(),
            request.amount(),
            request.items()
        );
    }

    @Override
    public String processPayment(String orderId, BigDecimal amount) {
        if (simulatePaymentFailure && paymentCallCount.incrementAndGet() <= 2) {
            // Simulate a transient failure — Temporal will retry per the ActivityOptions
            // configured in OrderWorkflowImpl. Watch the Temporal Web UI to see retries.
            log.warn("Simulated payment failure attempt {} for orderId={}",
                paymentCallCount.get(), orderId);
            throw new RuntimeException("Payment service unavailable — simulated failure");
        }

        String transactionId = "TXN-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        log.info("Payment processed orderId={} txnId={}", orderId, transactionId);
        return transactionId;
    }

    @Override
    public String fulfillOrder(String orderId) {
        // Retrieve heartbeat details from a previous attempt, if any.
        // If this activity was rescheduled after a heartbeat timeout, Temporal passes back
        // whatever was in the last heartbeat() call — allowing the activity to resume
        // from where it left off rather than starting over.
        Optional<Integer> lastStep = Activity.getExecutionContext().getHeartbeatDetails(Integer.class);
        int startFrom = lastStep.orElse(0);

        log.info("Fulfilling order orderId={} resuming from step={}", orderId, startFrom);

        // Simulate three fulfillment steps (e.g., warehouse pick, pack, ship)
        for (int step = startFrom; step < 3; step++) {
            try {
                // Simulate work — replace with real I/O in production
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Heartbeat reports progress and proves the activity is alive.
            // If the worker crashes here, the next attempt resumes from 'step + 1'.
            Activity.getExecutionContext().heartbeat(step + 1);
            log.info("Fulfillment step {} complete for orderId={}", step + 1, orderId);
        }

        String trackingNumber = "TRACK-" + orderId.toUpperCase();
        log.info("Order fulfilled orderId={} tracking={}", orderId, trackingNumber);
        return trackingNumber;
    }

    @Override
    public void sendNotification(String orderId, String trackingNumber) {
        log.info("Sending notification for orderId={} tracking={}", orderId, trackingNumber);
        // Mocked — in production this would call an email/SMS service
        log.info("Notification sent for orderId={}", orderId);
    }
}
