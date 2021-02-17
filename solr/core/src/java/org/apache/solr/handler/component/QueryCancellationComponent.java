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

import org.apache.lucene.search.CancellableTask;

import java.io.IOException;

/** Responsible for handling query cancellation requests */
public class QueryCancellationComponent extends SearchComponent {
    public static final String COMPONENT_NAME = "querycancellation";

    private boolean shouldProcess;

    @Override
    public void prepare(ResponseBuilder rb) throws IOException
    {
        if (rb.isCancellation()) {
            shouldProcess = true;
        }
    }

    @Override
    public void process(ResponseBuilder rb) {
        if (!shouldProcess) {
            return;
        }

        String cancellationUUID = rb.getCancellationUUID();

        if (cancellationUUID == null) {
            throw new RuntimeException("Null query UUID seen");
        }

        CancellableTask cancellableTask = rb.req.getCore().getCancellableTask(cancellationUUID);

        if (cancellableTask != null) {
            cancellableTask.cancelTask();
            rb.rsp.add("cancellationResult", "success");
        } else {
            rb.rsp.add("cancellationResult", "not found");
        }
    }

    @Override
    public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
        if (!shouldProcess) {
            return;
        }

        boolean failureSeen = false;
        boolean queryNotFound = false;

        for (ShardResponse r : sreq.responses) {

            String cancellationResult = (String) r.getSolrResponse()
                    .getResponse().get("cancellationResult");

            if (!cancellationResult.equalsIgnoreCase("success")) {
                if (cancellationResult.equalsIgnoreCase("not found")) {
                    queryNotFound = true;
                } else {
                    failureSeen = true;
                }

                break;
            }
        }

        if (failureSeen) {
            rb.rsp.getValues().add("status", "Query with queryID " + rb.getCancellationUUID() +
                    " could not be cancelled successfully");
        } else if (queryNotFound) {
            rb.rsp.getValues().add("status", "Query with queryID " + rb.getCancellationUUID() +
                    " not found");
        } else {
            rb.rsp.getValues().add("status", "Query with queryID " + rb.getCancellationUUID() +
                    " cancelled successfully");
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
