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
import java.util.List;

import static org.apache.solr.common.params.CommonParams.DISTRIB;

/**
 * Abstract class which serves as the root of all task managing handlers
 */
public abstract class TaskManagementHandler extends RequestHandlerBase implements SolrCoreAware, PermissionNameProvider {
    private ShardHandlerFactory shardHandlerFactory;

    @SuppressWarnings("unchecked")
    @Override
    public abstract void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception;

    @Override
    public void inform(SolrCore core) {
        this.shardHandlerFactory = core.getCoreContainer().getShardHandlerFactory();
    }

    protected void processRequest(ResponseBuilder rb) throws IOException {
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

