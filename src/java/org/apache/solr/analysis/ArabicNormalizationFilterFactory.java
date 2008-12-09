package org.apache.solr.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ar.ArabicNormalizationFilter;


/**
 *
 *
 **/
public class ArabicNormalizationFilterFactory extends BaseTokenFilterFactory{

  public ArabicNormalizationFilter create(TokenStream input) {
    return new ArabicNormalizationFilter(input);
  }
}
