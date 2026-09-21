package com.example.temporal.workflow;

import com.example.temporal.activity.OrderActivities;
import com.example.temporal.model.OrderRequest;
import com.example.temporal.model.OrderResult;
import com.example.temporal.model.ValidatedOrder;
import io.temporal.client.WorkflowClient;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.client.WorkflowFailedException;
import io.temporal.client.WorkflowOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import io.temporal.workflow.Workflow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OrderWorkflowTest {

    private TestWorkflowEnvironment testEnv;
    private Worker worker;
    private OrderActivities mockActivities;

    @BeforeEach
    void setUp() {
        // TestWorkflowEnvironment spins up an in-process Temporal server with simulated time.
        // No real Temporal server or network is required.
        testEnv = TestWorkflowEnvironment.newInstance();
        worker = testEnv.newWorker(OrderWorkflow.TASK_QUEUE);

        // Register the real workflow implementation — what we're actually testing
        worker.registerWorkflowImplementationTypes(OrderWorkflowImpl.class);

        // Mock the activities so tests control every outcome without side effects.
        // withoutAnnotations() prevents Mockito from inheriting @ActivityMethod on the
        // generated proxy class, which Temporal rejects (the annotation is only valid on
        // interface methods, not concrete implementations).
        mockActivities = mock(OrderActivities.class, withSettings().withoutAnnotations());
        worker.registerActivitiesImplementations(mockActivities);

        testEnv.start();
    }

    @AfterEach
    void tearDown() {
        testEnv.close();
    }

    // Convenience: create a workflow stub with a fresh random ID so tests don't collide
    private OrderWorkflow newStub() {
        return testEnv.getWorkflowClient().newWorkflowStub(
            OrderWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(OrderWorkflow.TASK_QUEUE)
                .setWorkflowId("test-order-" + UUID.randomUUID())
                .setWorkflowExecutionTimeout(Duration.ofMinutes(10))
                .build()
        );
    }

    private OrderRequest sampleRequest() {
        return new OrderRequest("order-1", "customer-1", new BigDecimal("99.99"), List.of("item-1"));
    }

    @Test
    void testHappyPath() {
        OrderRequest request = sampleRequest();
        ValidatedOrder validated = new ValidatedOrder(
            request.orderId(), request.customerId(), request.amount(), request.items());

        when(mockActivities.validateOrder(any())).thenReturn(validated);
        when(mockActivities.processPayment(any(), any())).thenReturn("TXN-001");
        when(mockActivities.fulfillOrder(any())).thenReturn("TRACK-001");
        doNothing().when(mockActivities).sendNotification(any(), any());

        OrderResult result = newStub().processOrder(request);

        assertThat(result.status()).isEqualTo("SUCCESS");
        assertThat(result.trackingNumber()).isEqualTo("TRACK-001");
        assertThat(result.transactionId()).isEqualTo("TXN-001");
        assertThat(result.errorMessage()).isNull();
    }

    @Test
    void testPaymentRetryThenSuccess() {
        // Demonstrates Temporal's retry policy: processPayment is configured with
        // maxAttempts=5. Here it fails twice and succeeds on the third call.
        OrderRequest request = sampleRequest();
        ValidatedOrder validated = new ValidatedOrder(
            request.orderId(), request.customerId(), request.amount(), request.items());

        when(mockActivities.validateOrder(any())).thenReturn(validated);
        when(mockActivities.processPayment(any(), any()))
            .thenThrow(new RuntimeException("Payment gateway timeout"))
            .thenThrow(new RuntimeException("Payment gateway timeout"))
            .thenReturn("TXN-RETRY-001");  // succeeds on 3rd attempt
        when(mockActivities.fulfillOrder(any())).thenReturn("TRACK-001");
        doNothing().when(mockActivities).sendNotification(any(), any());

        OrderResult result = newStub().processOrder(request);

        assertThat(result.status()).isEqualTo("SUCCESS");
        assertThat(result.transactionId()).isEqualTo("TXN-RETRY-001");
        // Verify Temporal actually retried — processPayment must have been called 3 times
        verify(mockActivities, times(3)).processPayment(any(), any());
    }

    @Test
    void testPaymentPermanentFailure() {
        // Demonstrates retry exhaustion: processPayment always throws, so Temporal
        // retries up to maxAttempts=5, then wraps the last exception in ActivityFailure.
        // The workflow catches ActivityFailure and returns a FAILED OrderResult.
        OrderRequest request = sampleRequest();
        ValidatedOrder validated = new ValidatedOrder(
            request.orderId(), request.customerId(), request.amount(), request.items());

        when(mockActivities.validateOrder(any())).thenReturn(validated);
        when(mockActivities.processPayment(any(), any()))
            .thenThrow(new RuntimeException("Payment service permanently down"));

        OrderResult result = newStub().processOrder(request);

        assertThat(result.status()).isEqualTo("FAILED");
        assertThat(result.errorMessage()).contains("Payment service permanently down");
        assertThat(result.trackingNumber()).isNull();
        // Temporal exhausted all 5 configured attempts before giving up
        verify(mockActivities, times(5)).processPayment(any(), any());
    }

    @Test
    void testWorkflowTimeout() throws Exception {
        // Demonstrates the workflow execution timeout (schedule-to-close at the workflow level).
        // Uses a separate TestWorkflowEnvironment so the sleeping workflow doesn't bleed into
        // other tests. A factory registers a workflow impl that calls Workflow.sleep() — this
        // respects TestWorkflowEnvironment's simulated clock, unlike Thread.sleep().
        try (TestWorkflowEnvironment timeoutEnv = TestWorkflowEnvironment.newInstance()) {
            Worker timeoutWorker = timeoutEnv.newWorker(OrderWorkflow.TASK_QUEUE);

            // Register a factory (lambda) instead of a class so we can inline the implementation.
            // Workflow.sleep() yields to the Temporal scheduler and resumes when simulated
            // time advances — the execution timeout fires before the sleep completes.
            timeoutWorker.registerWorkflowImplementationFactory(
                OrderWorkflow.class,
                () -> request -> {
                    Workflow.sleep(Duration.ofHours(1)); // far longer than the 5s timeout below
                    return OrderResult.failure(request.orderId(), "unreachable");
                }
            );
            timeoutEnv.start();

            // Configure a short execution timeout so the test runs quickly in simulated time
            OrderWorkflow stub = timeoutEnv.getWorkflowClient().newWorkflowStub(
                OrderWorkflow.class,
                WorkflowOptions.newBuilder()
                    .setTaskQueue(OrderWorkflow.TASK_QUEUE)
                    .setWorkflowId("timeout-test-" + UUID.randomUUID())
                    .setWorkflowExecutionTimeout(Duration.ofSeconds(5))
                    .build()
            );

            // Start the workflow asynchronously so we can advance time before it completes
            WorkflowExecution execution = WorkflowClient.start(stub::processOrder, sampleRequest());

            // Skip simulated time past the 5-second execution timeout
            timeoutEnv.sleep(Duration.ofSeconds(6));

            // Retrieve an untyped stub tied to the running execution to fetch its final result.
            // Getting the result of a timed-out workflow throws WorkflowFailedException.
            io.temporal.client.WorkflowStub untypedStub = timeoutEnv.getWorkflowClient()
                .newUntypedWorkflowStub(execution.getWorkflowId());
            assertThrows(WorkflowFailedException.class,
                () -> untypedStub.getResult(OrderResult.class));
        }
    }

    @Test
    void testHeartbeatTimeout() {
        // Demonstrates what happens when fulfillOrder exhausts its retries due to
        // heartbeat timeouts: the workflow's ActivityFailure handler returns FAILED.
        //
        // Note: directly simulating a heartbeat timeout in TestWorkflowEnvironment requires
        // a blocking real thread, which adds complexity. Instead, we mock fulfillOrder to
        // throw on all 3 configured attempts — the same end-state a heartbeat timeout
        // produces after retries are exhausted. The actual heartbeat mechanism is shown
        // in OrderActivitiesImpl.fulfillOrder() and observable via the Temporal Web UI.
        OrderRequest request = sampleRequest();
        ValidatedOrder validated = new ValidatedOrder(
            request.orderId(), request.customerId(), request.amount(), request.items());

        when(mockActivities.validateOrder(any())).thenReturn(validated);
        when(mockActivities.processPayment(any(), any())).thenReturn("TXN-HB-001");
        when(mockActivities.fulfillOrder(any()))
            .thenThrow(new RuntimeException("Simulated: activity heartbeat timed out"))
            .thenThrow(new RuntimeException("Simulated: activity heartbeat timed out"))
            .thenThrow(new RuntimeException("Simulated: activity heartbeat timed out"));

        OrderResult result = newStub().processOrder(request);

        assertThat(result.status()).isEqualTo("FAILED");
        assertThat(result.errorMessage()).contains("heartbeat timed out");
        // fulfillOrder was retried the full 3 configured attempts before the workflow gave up
        verify(mockActivities, times(3)).fulfillOrder(any());
    }
}
