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

import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.solr.common.params.CommonParams.TASK_CHECK_UUID;

/**
 * Handles request for listing all active cancellable tasks
 */
public class ActiveTasksListHandler extends TaskManagementHandler {
    // This can be a parent level member but we keep it here to allow future handlers to have
    // a custom list of components
    private List<SearchComponent> components;

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        Map<String, String> extraParams = null;
        ResponseBuilder rb = buildResponseBuilder(req, rsp, getComponentsList());

        rb.setIsTaskListRequest(true);

        String taskStatusCheckUUID = req.getParams().get(TASK_CHECK_UUID, null);

        if (taskStatusCheckUUID != null) {
            if (rb.isDistrib) {
                extraParams = new HashMap<>();

                extraParams.put(TASK_CHECK_UUID, taskStatusCheckUUID);
            }

            rb.setTaskStatusCheckUUID(taskStatusCheckUUID);
        }

        processRequest(req, rb, extraParams);
    }

    @Override
    public String getDescription() {
        return "activetaskslist";
    }

    @Override
    public Category getCategory() {
        return Category.ADMIN;
    }

    @Override
    public PermissionNameProvider.Name getPermissionName(AuthorizationContext ctx) {
        return PermissionNameProvider.Name.READ_PERM;
    }

    @Override
    public SolrRequestHandler getSubHandler(String path) {
        if (path.startsWith("/tasks/list")) {
            return this;
        }

        return null;
    }

    @Override
    public Boolean registerV2() {
        return Boolean.TRUE;
    }

    @Override
    public Collection<Api> getApis() {
        return ApiBag.wrapRequestHandlers(this, "core.tasks.list");
    }

    private List<SearchComponent> getComponentsList() {
        if (components == null) {
            components = buildComponentsList();
        }

        return components;
    }
}
