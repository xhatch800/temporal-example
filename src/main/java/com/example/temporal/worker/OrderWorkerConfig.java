package com.example.temporal.worker;

import com.example.temporal.activity.OrderActivitiesImpl;
import com.example.temporal.workflow.OrderWorkflow;
import com.example.temporal.workflow.OrderWorkflowImpl;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

// ApplicationReadyEvent fires after the full Spring context is up — ensures that
// all beans (including TemporalConfig's WorkerFactory) are available before we start polling.
@Component
public class OrderWorkerConfig implements ApplicationListener<ApplicationReadyEvent> {

    private static final Logger log = LoggerFactory.getLogger(OrderWorkerConfig.class);

    private final WorkerFactory workerFactory;
    private final OrderActivitiesImpl orderActivities;

    public OrderWorkerConfig(WorkerFactory workerFactory, OrderActivitiesImpl orderActivities) {
        this.workerFactory = workerFactory;
        this.orderActivities = orderActivities;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        // A Worker polls a specific task queue for workflow and activity tasks.
        // All workers sharing the same task queue form a pool — Temporal load-balances across them.
        Worker worker = workerFactory.newWorker(OrderWorkflow.TASK_QUEUE);

        // Register the workflow class (not an instance) — Temporal creates a new instance
        // per workflow execution to maintain determinism.
        worker.registerWorkflowImplementationTypes(OrderWorkflowImpl.class);

        // Register the activity instance (Spring bean) — activities are stateful and
        // may use @Value, loggers, or other injected components.
        worker.registerActivitiesImplementations(orderActivities);

        // Start all registered workers and begin polling for tasks
        workerFactory.start();
        log.info("Temporal worker started on task queue '{}'", OrderWorkflow.TASK_QUEUE);
    }
}
