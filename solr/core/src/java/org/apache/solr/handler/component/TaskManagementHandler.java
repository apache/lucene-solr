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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.plugin.SolrCoreAware;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.PATH;

/**
 * Abstract class which serves as the root of all task managing handlers
 */
public abstract class TaskManagementHandler extends RequestHandlerBase implements SolrCoreAware, PermissionNameProvider {
    private ShardHandlerFactory shardHandlerFactory;

    @Override
    public void inform(SolrCore core) {
        this.shardHandlerFactory = core.getCoreContainer().getShardHandlerFactory();
    }

    /**
     * Process the actual request.
     * extraParams is required for allowing sub handlers to pass in custom parameters to be put in the
     * outgoing shard request
     */
    protected void processRequest(SolrQueryRequest req, ResponseBuilder rb, Map<String, String> extraParams) throws IOException {
        ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
        List<SearchComponent> components = rb.components;

        shardHandler.prepDistributed(rb);

        for(SearchComponent c : components) {
            c.prepare(rb);
        }

        if (!rb.isDistrib) {
            for (SearchComponent component : components) {
                component.process(rb);
            }
        } else {
            ShardRequest sreq = new ShardRequest();

            // Distribute to all shards
            sreq.shards = rb.shards;
            sreq.actualShards = sreq.shards;

            sreq.responses = new ArrayList<>(sreq.actualShards.length);
            rb.finished = new ArrayList<>();

            for (String shard : sreq.actualShards) {
                ModifiableSolrParams params = new ModifiableSolrParams(sreq.params);
                String reqPath = (String) req.getContext().get(PATH);

                params.set(CommonParams.QT, reqPath);
                params.remove(ShardParams.SHARDS);      // not a top-level request
                params.set(DISTRIB, "false");               // not a top-level request
                params.remove("indent");
                params.remove(CommonParams.HEADER_ECHO_PARAMS);
                params.set(ShardParams.IS_SHARD, true);  // a sub (shard) request
                params.set(ShardParams.SHARDS_PURPOSE, sreq.purpose);
                params.set(ShardParams.SHARD_URL, shard); // so the shard knows what was asked
                params.set(CommonParams.OMIT_HEADER, false);

                if (extraParams != null) {
                    for(Map.Entry<String, String> entry : extraParams.entrySet()) {
                        params.set(entry.getKey(), entry.getValue());
                    }
                }

                shardHandler.submit(sreq, shard, params);
            }

            ShardResponse srsp = shardHandler.takeCompletedOrError();

            if (srsp.getException() != null) {
                shardHandler.cancelAll();
                if (srsp.getException() instanceof SolrException) {
                    throw (SolrException) srsp.getException();
                } else {
                    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, srsp.getException());
                }
            }

            rb.finished.add(srsp.getShardRequest());

            for (SearchComponent c : components) {
                c.handleResponses(rb, srsp.getShardRequest());
            }
        }
    }

    public static List<SearchComponent> buildComponentsList() {
        List<SearchComponent> components = new ArrayList<>(2);

        QueryCancellationComponent component = new QueryCancellationComponent();
        components.add(component);

        ActiveTasksListComponent activeTasksListComponent = new ActiveTasksListComponent();
        components.add(activeTasksListComponent);

        return components;
    }

    public static ResponseBuilder buildResponseBuilder(SolrQueryRequest req, SolrQueryResponse rsp,
                                                       List<SearchComponent> components) {
        CoreContainer cc = req.getCore().getCoreContainer();
        boolean isZkAware = cc.isZooKeeperAware();

        ResponseBuilder rb = new ResponseBuilder(req, rsp, components);

        rb.isDistrib = req.getParams().getBool(DISTRIB, isZkAware);

        return rb;
    }
}

