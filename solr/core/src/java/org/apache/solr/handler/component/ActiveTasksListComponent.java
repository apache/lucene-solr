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
    public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
        if (!shouldProcess) {
            return;
        }

        NamedList<String> resultList = new NamedList<>();

        for (ShardResponse r : sreq.responses) {

            NamedList<String> result = (NamedList<String>) r.getSolrResponse()
                    .getResponse().get("taskList");

            Iterator<Map.Entry<String, String>> iterator = result.iterator();

            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();

                resultList.add(entry.getKey(), entry.getValue());
            }
        }

        rb.rsp.getValues().add("taskList", resultList);
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
