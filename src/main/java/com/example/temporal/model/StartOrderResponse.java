package com.example.temporal.model;

// 202 response returned immediately after the workflow is submitted — the workflow runs async
public record StartOrderResponse(
    String workflowId,
    String runId,
    String status   // always "STARTED"
) {}
