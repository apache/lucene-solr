package org.apache.solr.core;

import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;


/**
 *
 *
 **/
public class MockQuerySenderListenerReqHandler extends RequestHandlerBase {
  public SolrQueryRequest req;
  public SolrQueryResponse rsp;

  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    this.req = req;
    this.rsp = rsp;
  }

  public String getDescription() {
    String result = null;
    return result;
  }

  public String getSourceId() {
    String result = null;
    return result;
  }

  public String getSource() {
    String result = null;
    return result;
  }

  public String getVersion() {
    String result = null;
    return result;
  }
}
