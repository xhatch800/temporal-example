package com.example.temporal.config;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.WorkerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TemporalConfig {

    // Injected from application.yml — defaults to localhost:7233 so the app works
    // with both 'temporal server start-dev' and the provided docker-compose.
    @Value("${temporal.service-address:localhost:7233}")
    private String serviceAddress;

    // WorkflowServiceStubs is the gRPC transport layer.
    // It manages the channel to the Temporal server and handles reconnects.
    @Bean
    public WorkflowServiceStubs workflowServiceStubs() {
        return WorkflowServiceStubs.newServiceStubs(
            WorkflowServiceStubsOptions.newBuilder()
                .setTarget(serviceAddress)
                .build()
        );
    }

    // WorkflowClient is used to start, signal, and query workflows.
    // The controller autowires this to submit new orders.
    @Bean
    public WorkflowClient workflowClient(WorkflowServiceStubs stubs) {
        return WorkflowClient.newInstance(stubs);
    }

    // WorkerFactory manages the lifecycle of one or more Workers.
    // call workerFactory.start() once all workers are registered.
    @Bean
    public WorkerFactory workerFactory(WorkflowClient workflowClient) {
        return WorkerFactory.newInstance(workflowClient);
    }
}
