package org.apache.solr.spelling.suggest;

public final class TermFreq {
  public final String term;
  public final float v;

  public TermFreq(String term, float v) {
    this.term = term;
    this.v = v;
  }
}