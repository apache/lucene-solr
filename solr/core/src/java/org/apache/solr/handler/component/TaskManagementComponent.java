package org.apache.solr.handler.component;

import org.apache.lucene.search.CancellableTask;

import java.io.IOException;

public class TaskManagementComponent extends SearchComponent {
    public static final String COMPONENT_NAME = "taskmanagement";

    private boolean shouldProcess;

    @Override
    public void prepare(ResponseBuilder rb) throws IOException {
        if (rb.isTaskListRequest()) {
            shouldProcess = true;
        }
    }

    @Override
    public void process(ResponseBuilder rb) {
        if (!shouldProcess) {
            return;
        }

        CancellableTask cancellableTask = rb.req.getCore().getCancellableTask(rb.getCancellationUUID());

        if (cancellableTask != null) {
            cancellableTask.cancelTask();
        }
    }

    @Override
    public String getDescription() {
        return "querycancellation";
    }

    @Override
    public Category getCategory() {
        return Category.OTHER;
    }
}
