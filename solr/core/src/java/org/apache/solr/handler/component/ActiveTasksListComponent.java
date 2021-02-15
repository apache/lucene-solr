package org.apache.solr.handler.component;

import org.apache.solr.common.util.NamedList;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/** List the active tasks that can be cancelled */
public class ActiveTasksListComponent extends SearchComponent {
    public static final String COMPONENT_NAME = "activetaskslistcomponent";

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

        NamedList<String> temp = new NamedList<>();

        Iterator<Map.Entry<String, String>> iterator = rb.req.getCore().getActiveQueriesGenerated();

        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            temp.add(entry.getKey(), entry.getValue());
        }

        rb.rsp.add("taskList", temp);
    }

    @Override
    public String getDescription() {
        return "activetaskslist";
    }

    @Override
    public Category getCategory() {
        return Category.OTHER;
    }
}
