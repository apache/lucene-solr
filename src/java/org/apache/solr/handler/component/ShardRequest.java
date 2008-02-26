package org.apache.solr.handler.component;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;

import java.util.ArrayList;
import java.util.List;


// todo... when finalized make accessors
public class ShardRequest {
  public final static String[] ALL_SHARDS = null;

  public final static int PURPOSE_PRIVATE         = 0x01;
  public final static int PURPOSE_GET_TERM_DFS    = 0x02;
  public final static int PURPOSE_GET_TOP_IDS     = 0x04;
  public final static int PURPOSE_REFINE_TOP_IDS  = 0x08;
  public final static int PURPOSE_GET_FACETS      = 0x10;
  public final static int PURPOSE_REFINE_FACETS   = 0x20;
  public final static int PURPOSE_GET_FIELDS      = 0x40;
  public final static int PURPOSE_GET_HIGHLIGHTS  = 0x80;
  public final static int PURPOSE_GET_DEBUG       =0x100;

  public int purpose;  // the purpose of this request

  public String[] shards;  // the shards this request should be sent to, null for all
// TODO: how to request a specific shard address?


  public ModifiableSolrParams params;


  /** list of responses... filled out by framework */
  public List<ShardResponse> responses = new ArrayList<ShardResponse>();

  /** actual shards to send the request to, filled out by framework */
  public String[] actualShards;

  // TODO: one could store a list of numbers to correlate where returned docs
  // go in the top-level response rather than looking up by id...
  // this would work well if we ever transitioned to using internal ids and
  // didn't require a uniqueId

  public String toString() {
    return "ShardRequest:{params=" + params
            + ", purpose=" + Integer.toHexString(purpose)
            + ", nResponses =" + responses.size()
            + "}";
  }
}


class ShardResponse {
  public ShardRequest req;
  public String shard;
  public String shardAddress;  // the specific shard that this response was received from  
  public int rspCode;
  public Throwable exception;
  public SolrResponse rsp;

  public String toString() {
    return "ShardResponse:{shard="+shard+",shardAddress="+shardAddress
            +"\n\trequest=" + req
            +"\n\tresponse=" + rsp
            + (exception==null ? "" : "\n\texception="+ SolrException.toStr(exception)) 
            +"\n}";
  }
}