
package org.apache.solr.client.solrj.impl;

public class ResponseAndException {

  private LBHttpSolrClient.Rsp rsp;
  private Exception ex;

  public ResponseAndException(LBHttpSolrClient.Rsp rsp, Exception ex) {
    this.rsp = rsp;
    this.ex = ex;
  }

  public LBHttpSolrClient.Rsp getRsp() {
    return rsp;
  }


  public void setEx(Exception ex) {
    this.ex = ex;
  }
}
