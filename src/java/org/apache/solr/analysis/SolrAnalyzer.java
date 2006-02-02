package org.apache.solr.analysis;

import org.apache.lucene.analysis.Analyzer;

/**
 * @author yonik
 * @version $Id$
 */
public abstract class SolrAnalyzer extends Analyzer {
  int posIncGap=0;
  
  public void setPositionIncrementGap(int gap) {
    posIncGap=gap;
  }

  public int getPositionIncrementGap(String fieldName) {
    return posIncGap;
  }
}
