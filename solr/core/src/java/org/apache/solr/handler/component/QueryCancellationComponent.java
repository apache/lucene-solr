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

import org.apache.solr.client.solrj.util.Cancellable;

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

        Cancellable cancellableTask = rb.req.getCore().getCancellableQueryTracker().getCancellableTask(cancellationUUID);

        if (cancellableTask != null) {
            cancellableTask.cancel();
            rb.rsp.add("cancellationResult", "success");
        } else {
            rb.rsp.add("cancellationResult", "not found");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
        if (!shouldProcess) {
            return;
        }

        boolean queryFound = false;

        for (ShardResponse r : sreq.responses) {

            String cancellationResult = (String) r.getSolrResponse()
                    .getResponse().get("cancellationResult");

            if (cancellationResult.equalsIgnoreCase("success")) {
                queryFound = true;

                break;
            }
        }

        // If any shard sees the query as present, then we mark the query as successfully cancelled. If no shard found
        // the query, then that can denote that the query was not found. This is important since the query cancellation
        // request is broadcast to all shards, and the query might have completed on some shards but not on others

        if(queryFound) {
            rb.rsp.getValues().add("status", "Query with queryID " + rb.getCancellationUUID() +
                    " cancelled successfully");
            rb.rsp.getValues().add("responseCode", 200 /* HTTP OK */);
        } else {
            rb.rsp.getValues().add("status", "Query with queryID " + rb.getCancellationUUID() +
                    " not found");
            rb.rsp.getValues().add("responseCode", 404 /* HTTP NOT FOUND */);
        }
    }

    @Override
    public String getDescription() {
        return "Supports cancellation of queries which are cancellable";
    }

    @Override
    public Category getCategory() {
        return Category.OTHER;
    }
}
