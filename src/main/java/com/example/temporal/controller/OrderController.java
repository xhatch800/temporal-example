package com.example.temporal.controller;

import com.example.temporal.model.OrderRequest;
import com.example.temporal.model.StartOrderResponse;
import com.example.temporal.workflow.OrderWorkflow;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final WorkflowClient workflowClient;

    public OrderController(WorkflowClient workflowClient) {
        this.workflowClient = workflowClient;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)   // 202: work accepted but not yet complete
    public StartOrderResponse startOrder(@RequestBody OrderRequest request) {

        WorkflowOptions options = WorkflowOptions.newBuilder()
            // Workflow ID is deterministic from the orderId — re-posting the same orderId
            // is idempotent: Temporal returns the existing execution instead of starting a duplicate.
            .setWorkflowId("order-" + request.orderId())
            .setTaskQueue(OrderWorkflow.TASK_QUEUE)
            // Workflow-level deadline: the entire order process must finish within 10 minutes
            .setWorkflowExecutionTimeout(Duration.ofMinutes(10))
            .build();

        OrderWorkflow stub = workflowClient.newWorkflowStub(OrderWorkflow.class, options);

        // WorkflowClient.start() dispatches the workflow and returns immediately.
        // The workflow runs asynchronously on the worker — this thread does not wait.
        WorkflowExecution execution = WorkflowClient.start(stub::processOrder, request);

        return new StartOrderResponse(
            execution.getWorkflowId(),
            execution.getRunId(),
            "STARTED"
        );
    }
}
