package org.apache.solr.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ar.ArabicStemFilter;


/**
 *
 *
 **/
public class ArabicStemFilterFactory extends BaseTokenFilterFactory{


  public TokenStream create(TokenStream input) {
    return new ArabicStemFilter(input);
  }
}
