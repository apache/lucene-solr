package org.apache.solr.schema;

import org.apache.lucene.search.DefaultSimilarity;

public class MockConfigurableSimilarity extends DefaultSimilarity {
  private String passthrough;

  public MockConfigurableSimilarity(String passthrough) {
    this.passthrough = passthrough;
  }

  public String getPassthrough() {
    return passthrough;
  }
}
