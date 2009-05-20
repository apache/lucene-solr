package org.apache.solr.handler.clustering;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.search.DocSet;


/**
 *
 *
 **/
public class MockDocumentClusteringEngine extends DocumentClusteringEngine{
  public NamedList cluster(DocSet docs, SolrParams solrParams) {
    NamedList result = new NamedList();
    return result;
  }

  public NamedList cluster(SolrParams solrParams) {
    NamedList result = new NamedList();
    return result;
  }
}
