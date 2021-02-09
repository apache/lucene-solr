package org.apache.solr.handler.component;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.plugin.SolrCoreAware;

import java.util.ArrayList;
import java.util.List;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.QUERY_CANCELLATION_UUID;
import static org.apache.solr.handler.component.ShardRequest.PURPOSE_CANCEL_TASK;

public class TaskManagementHandler extends RequestHandlerBase implements SolrCoreAware, PermissionNameProvider {
    private enum TaskRequestType {
        TASK_CANCEL,
        TASK_LIST
    };

    private ShardHandlerFactory shardHandlerFactory;
    private TaskRequestType taskRequestType;


    @SuppressWarnings("unchecked")
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        CoreContainer cc = req.getCore().getCoreContainer();
        boolean isZkAware = cc.isZooKeeperAware();
        boolean isDistrib = req.getParams().getBool(DISTRIB, isZkAware);

        List<SearchComponent> components = buildComponentsList();
        ResponseBuilder rb = new ResponseBuilder(req, rsp, components);

        ShardHandler shardHandler = shardHandlerFactory.getShardHandler();

        shardHandler.prepDistributed(rb);

        String cancellationUUID = req.getParams().get(QUERY_CANCELLATION_UUID, null);

        if (cancellationUUID == null) {
            throw new IllegalArgumentException("Query cancellation was requested but no query UUID for cancellation was given");
        }

        rb.setCancellationUUID(cancellationUUID);

        if (taskRequestType == TaskRequestType.TASK_CANCEL) {
            rb.setCancellation(true);
        } else if (taskRequestType == TaskRequestType.TASK_LIST) {
            rb.setTaskListRequest(true);
        }

        for(SearchComponent c : components ) {
            c.prepare(rb);
        }

        if (!isDistrib) {
            for (SearchComponent component : components) {
                component.process(rb);
            }
        } else {
            ShardRequest sreq = new ShardRequest();
            sreq.purpose = PURPOSE_CANCEL_TASK;

            // Distribute to all shards
            sreq.shards = rb.shards;
            sreq.actualShards = sreq.shards;

            sreq.responses = new ArrayList<>(sreq.actualShards.length);
            rb.finished = new ArrayList<>();

            for (String shard : sreq.actualShards) {
                ModifiableSolrParams params = new ModifiableSolrParams(sreq.params);
                params.remove(ShardParams.SHARDS);      // not a top-level request
                params.set(DISTRIB, "false");               // not a top-level request
                params.remove("indent");
                params.remove(CommonParams.HEADER_ECHO_PARAMS);
                params.set(ShardParams.IS_SHARD, true);  // a sub (shard) request
                params.set(ShardParams.SHARDS_PURPOSE, sreq.purpose);
                params.set(ShardParams.SHARD_URL, shard); // so the shard knows what was asked
                params.set(CommonParams.OMIT_HEADER, false);

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
        }

        rsp.getValues().add("status", "query with queryID " + rb.getCancellationUUID() + " " + "cancelled");
    }

    @Override
    public void inform(SolrCore core) {
        this.shardHandlerFactory = core.getCoreContainer().getShardHandlerFactory();
    }

    @Override
    public String getDescription() {
        return "Cancel queries";
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
        if (path.equals("/tasks/cancel")) {
            taskRequestType = TaskRequestType.TASK_CANCEL;
            return this;
        } else if (path.equals("/tasks/list")) {
            taskRequestType = TaskRequestType.TASK_LIST;
            return this;
        }
        return null;
    }

    private List<SearchComponent> buildComponentsList() {
        List<SearchComponent> components = new ArrayList<>(1);

        QueryCancellationComponent component = new QueryCancellationComponent();
        components.add(component);

        TaskManagementComponent taskManagementComponent = new TaskManagementComponent();
        components.add(taskManagementComponent);

        return components;
    }
}

