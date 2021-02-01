package org.apache.solr.handler.component;

import org.apache.lucene.search.CancellableTask;

import java.io.IOException;

/** Responsible for handling query cancellation requests */
public class QueryCancellationComponent extends SearchComponent {

    public static final String COMPONENT_NAME = "querycancellation";

    private boolean shouldProcess;

    @Override
    public void prepare(ResponseBuilder rb) throws IOException
    {
        if (rb.isDistrib && rb.isCancellation()) {
            shouldProcess = true;
        }
    }

    @Override
    public void process(ResponseBuilder rb) {
        if (!shouldProcess) {
            return;
        }

        CancellableTask cancellableTask = rb.req.getCore().getCancellableTask(rb.getCancellationUUID());

        cancellableTask.cancelTask();
    }

    @Override
    public String getDescription() {
        return "Handle query cancellation";
    }

    @Override
    public Category getCategory() {
        return Category.OTHER;
    }
}
