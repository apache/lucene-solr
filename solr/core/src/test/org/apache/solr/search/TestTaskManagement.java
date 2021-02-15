package org.apache.solr.search;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTaskManagement extends SolrCloudTestCase {
    private CloudSolrClient client;

    @BeforeClass
    public static void setupCluster() throws Exception {
        configureCluster(4)
                .addConfig("conf", configset("sql"))
                .configure();

        String collection = "collection1";

        CollectionAdminRequest.createCollection(collection, "conf", 2, 1)
                .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
                .process(cluster.getSolrClient());
        cluster.waitForActiveCollection(collection, 2, 2);
    }

    @Before
    public void setupCollection() throws Exception {
        client = cluster.getSolrClient();
        client.setDefaultCollection("collection1");
    }

    @Test
    public void testBasic() throws Exception {
        ModifiableSolrParams params = new ModifiableSolrParams();

        params.set("cancelUUID", "foobar");
        @SuppressWarnings({"rawtypes"})
        SolrRequest request = new QueryRequest(params);
        request.setPath("/tasks/cancel");

        client.request(request);
        int i = 0;
    }
}
