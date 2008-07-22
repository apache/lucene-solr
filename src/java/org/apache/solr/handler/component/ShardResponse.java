package org.apache.solr.handler.component;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrException;

public final class ShardResponse {
	  private ShardRequest req;
	  private String shard;
	  private String shardAddress;  // the specific shard that this response was received from  
	  private int rspCode;
	  private Throwable exception;
	  private SolrResponse rsp;

	  public String toString() {
	    return "ShardResponse:{shard="+shard+",shardAddress="+shardAddress
	            +"\n\trequest=" + req
	            +"\n\tresponse=" + rsp
	            + (exception==null ? "" : "\n\texception="+ SolrException.toStr(exception)) 
	            +"\n}";
	  }
	  
	  public Throwable getException()
	  {
		  return exception;
	  }
	  
	  public ShardRequest getShardRequest()
	  {
		  return req;
	  }
	  
	  public SolrResponse getSolrResponse()
	  {
		  return rsp;
	  }
	  
	  public String getShard()
	  {
		  return shard;
	  }
	  
	  void setShardRequest(ShardRequest rsp)
	  {
		  this.req = rsp;
	  }
	  
	  void setSolrResponse(SolrResponse rsp)
	  {
		  this.rsp = rsp;
	  }
	  
	  void setShard(String shard)
	  {
		  this.shard = shard;
	  }
	  
	  void setException(Throwable exception)
	  {
		  this.exception = exception;
	  }
	  
	  void setResponseCode(int rspCode)
	  {
		  this.rspCode = rspCode;
	  }
}
