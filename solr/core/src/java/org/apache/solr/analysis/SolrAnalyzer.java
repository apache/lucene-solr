/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.analysis;

import org.apache.lucene.analysis.*;

import java.io.Reader;
import java.io.IOException;

/**
 *
 */
public abstract class SolrAnalyzer extends Analyzer {
  int posIncGap=0;
  
  public void setPositionIncrementGap(int gap) {
    posIncGap=gap;
  }

  @Override
  public int getPositionIncrementGap(String fieldName) {
    return posIncGap;
  }

  /** wrap the reader in a CharStream, if appropriate */
  public Reader charStream(Reader reader){
    return reader;
  }

  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    return getStream(fieldName, reader).getTokenStream();
  }

  public static class TokenStreamInfo {
    private final Tokenizer tokenizer;
    private final TokenStream tokenStream;
    public TokenStreamInfo(Tokenizer tokenizer, TokenStream tokenStream) {
      this.tokenizer = tokenizer;
      this.tokenStream = tokenStream;
    }
    public Tokenizer getTokenizer() { return tokenizer; }
    public TokenStream getTokenStream() { return tokenStream; }
  }


  public abstract TokenStreamInfo getStream(String fieldName, Reader reader);

  @Override
  public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
    // if (true) return tokenStream(fieldName, reader);
    TokenStreamInfo tsi = (TokenStreamInfo)getPreviousTokenStream();
    if (tsi != null) {
      tsi.getTokenizer().reset(charStream(reader));
      // the consumer will currently call reset() on the TokenStream to hit all the filters.
      // this isn't necessarily guaranteed by the APIs... but is currently done
      // by lucene indexing in DocInverterPerField, and in the QueryParser
      return tsi.getTokenStream();
    } else {
      tsi = getStream(fieldName, reader);
      setPreviousTokenStream(tsi);
      return tsi.getTokenStream();
    }
  }
}
