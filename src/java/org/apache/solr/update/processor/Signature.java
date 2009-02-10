package org.apache.solr.update.processor;

import org.apache.solr.common.params.SolrParams;

public abstract class Signature {
  public void init(SolrParams nl) {
  }

  abstract public void add(String content);
  
  abstract public byte[] getSignature();
}
