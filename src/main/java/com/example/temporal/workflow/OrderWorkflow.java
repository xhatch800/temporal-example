package com.example.temporal.workflow;

import com.example.temporal.model.OrderRequest;
import com.example.temporal.model.OrderResult;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

// @WorkflowInterface defines the contract for the entire order process.
// Temporal serialises all inputs/outputs to JSON — records are ideal.
@WorkflowInterface
public interface OrderWorkflow {

    // Shared constant used by the worker (registration), the workflow stub
    // (in the controller), and tests — keeps all three in sync.
    String TASK_QUEUE = "order-task-queue";

    // The single entry point. Returns OrderResult instead of throwing so that
    // business failures (payment declined, fulfillment error) are represented
    // as data rather than workflow-level exceptions.
    @WorkflowMethod
    OrderResult processOrder(OrderRequest request);
}
