/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.component;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.NamedList;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/** List the active tasks that can be cancelled */
public class ActiveTasksListComponent extends SearchComponent {
    public static final String COMPONENT_NAME = "activetaskslist";

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

        if (rb.getTaskStatusCheckUUID() != null) {
            boolean isActiveOnThisShard = rb.req.getCore().getCancellableQueryTracker().isQueryIdActive(rb.getTaskStatusCheckUUID());

            rb.rsp.add("taskStatus", isActiveOnThisShard);
            return;
        }

        rb.rsp.add("taskList", (MapWriter) ew -> {
            Iterator<Map.Entry<String, String>> iterator = rb.req.getCore().getCancellableQueryTracker().getActiveQueriesGenerated();

            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();
                ew.put(entry.getKey(), entry.getValue());
            }
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
        if (!shouldProcess) {
            return;
        }

        NamedList<String> resultList = new NamedList<>();

        for (ShardResponse r : sreq.responses) {

            if (rb.getTaskStatusCheckUUID() != null) {
                boolean isTaskActiveOnShard = r.getSolrResponse().getResponse().getBooleanArg("taskStatus");

                if (isTaskActiveOnShard) {
                    rb.rsp.getValues().add("taskStatus", "id:" + rb.getTaskStatusCheckUUID() + ", status: active");
                    return;
                } else {
                    continue;
                }
            }

            LinkedHashMap<String, String> result = (LinkedHashMap<String, String>) r.getSolrResponse()
                    .getResponse().get("taskList");

            Iterator<Map.Entry<String, String>> iterator = result.entrySet().iterator();

            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();

                resultList.add(entry.getKey(), entry.getValue());
            }
        }

        if (rb.getTaskStatusCheckUUID() != null) {
            // We got here with the specific taskID check being specified -- this means that the taskID was not
            // found in active tasks on any shard
            rb.rsp.getValues().add("taskStatus", "id:" + rb.getTaskStatusCheckUUID() + ", status: inactive");
            return;
        }

        rb.rsp.getValues().add("taskList", resultList);
    }

    @Override
    public String getDescription() {
        return "Responsible for listing all active cancellable tasks and also supports checking the status of " +
                "a particular task";
    }

    @Override
    public Category getCategory() {
        return Category.OTHER;
    }
}
