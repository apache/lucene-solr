package org.apache.solr.client.solrj.impl;

import org.apache.solr.common.util.RpcFactory;

public class RpcFactoryImpl implements RpcFactory  {
    private final CloudSolrClient cloudSolrClient;

    RpcFactoryImpl(CloudSolrClient cloudSolrClient) {
        this.cloudSolrClient = cloudSolrClient;
    }

    @Override
    //TODO
    public Rpc create() {
        return null;
    }

}
