package org.apache.solr.handler.component;


import org.apache.solr.common.params.ModifiableSolrParams;

public abstract class ShardHandler {
  public abstract void checkDistributed(ResponseBuilder rb);
  public abstract void submit(ShardRequest sreq, String shard, ModifiableSolrParams params) ;
  public abstract ShardResponse takeCompletedOrError();
  public abstract void cancelAll();
}
