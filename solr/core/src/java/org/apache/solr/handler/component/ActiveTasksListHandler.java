package org.apache.solr.handler.component;

import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;

import java.util.Collection;
import java.util.List;

/**
 * Handles request for listing all active cancellable tasks
 */
public class ActiveTasksListHandler extends TaskManagementHandler {
    private List<SearchComponent> components;

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        ResponseBuilder rb = buildResponseBuilder(req, rsp, getComponentsList());

        rb.setTaskListRequest(true);

        processRequest(rb);
    }

    @Override
    public String getDescription() {
        return "List active cancellable tasks";
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
        if (path.equals("/tasks/list")) {
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
