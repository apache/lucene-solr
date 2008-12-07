package org.apache.solr.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ar.ArabicLetterTokenizer;

import java.io.Reader;


/**
 *
 *
 **/
public class ArabicLetterTokenizerFactory extends BaseTokenizerFactory{

  public TokenStream create(Reader input) {
    return new ArabicLetterTokenizer(input);
  }
}
