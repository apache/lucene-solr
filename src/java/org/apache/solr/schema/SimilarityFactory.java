package org.apache.solr.schema;

import org.apache.lucene.search.Similarity;
import org.apache.solr.common.params.SolrParams;

public abstract class SimilarityFactory {
  protected SolrParams params;

  public void init(SolrParams params) { this.params = params; }
  public SolrParams getParams() { return params; }

  public abstract Similarity getSimilarity();
}
