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

package org.apache.solr.schema;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.io.StringReader;

/** <code>TextField</code> is the basic type for configurable text analysis.
 * Analyzers for field types using this implementation should be defined in the schema.
 *
 */
public class TextField extends FieldType {
  protected boolean autoGeneratePhraseQueries;

  @Override
  protected void init(IndexSchema schema, Map<String,String> args) {
    properties |= TOKENIZED;
    if (schema.getVersion()> 1.1f) properties &= ~OMIT_TF_POSITIONS;
    if (schema.getVersion() > 1.3f) {
      autoGeneratePhraseQueries = false;
    } else {
      autoGeneratePhraseQueries = true;
    }
    String autoGeneratePhraseQueriesStr = args.remove("autoGeneratePhraseQueries");
    if (autoGeneratePhraseQueriesStr != null)
      autoGeneratePhraseQueries = Boolean.parseBoolean(autoGeneratePhraseQueriesStr);
    super.init(schema, args);    
  }

  public boolean getAutoGeneratePhraseQueries() {
    return autoGeneratePhraseQueries;
  }

  @Override
  public SortField getSortField(SchemaField field, boolean reverse) {
    /* :TODO: maybe warn if isTokenized(), but doesn't use LimitTokenCountFilter in it's chain? */
    return getStringSort(field, reverse);
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    writer.writeStr(name, f.stringValue(), true);
  }

  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    return parseFieldQuery(parser, getQueryAnalyzer(), field.getName(), externalVal);
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return term.utf8ToString();
  }

  @Override
  public void setAnalyzer(Analyzer analyzer) {
    this.analyzer = analyzer;
  }

  @Override
  public void setQueryAnalyzer(Analyzer analyzer) {
    this.queryAnalyzer = analyzer;
  }

  static Query parseFieldQuery(QParser parser, Analyzer analyzer, String field, String queryText) {
    int phraseSlop = 0;
    boolean enablePositionIncrements = true;

    // most of the following code is taken from the Lucene QueryParser

    // Use the analyzer to get all the tokens, and then build a TermQuery,
    // PhraseQuery, or nothing based on the term count

    TokenStream source;
    try {
      source = analyzer.tokenStream(field, new StringReader(queryText));
      source.reset();
    } catch (IOException e) {
      throw new RuntimeException("Unable to initialize TokenStream to analyze query text", e);
    }
    CachingTokenFilter buffer = new CachingTokenFilter(source);
    CharTermAttribute termAtt = null;
    PositionIncrementAttribute posIncrAtt = null;
    int numTokens = 0;

    try {
      buffer.reset();
    } catch (IOException e) {
      throw new RuntimeException("Unable to initialize TokenStream to analyze query text", e);
    }

    if (buffer.hasAttribute(CharTermAttribute.class)) {
      termAtt = buffer.getAttribute(CharTermAttribute.class);
    }
    if (buffer.hasAttribute(PositionIncrementAttribute.class)) {
      posIncrAtt = buffer.getAttribute(PositionIncrementAttribute.class);
    }

    int positionCount = 0;
    boolean severalTokensAtSamePosition = false;

    boolean hasMoreTokens = false;
    if (termAtt != null) {
      try {
        hasMoreTokens = buffer.incrementToken();
        while (hasMoreTokens) {
          numTokens++;
          int positionIncrement = (posIncrAtt != null) ? posIncrAtt.getPositionIncrement() : 1;
          if (positionIncrement != 0) {
            positionCount += positionIncrement;
          } else {
            severalTokensAtSamePosition = true;
          }
          hasMoreTokens = buffer.incrementToken();
        }
      } catch (IOException e) {
        // ignore
      }
    }
    try {
      // rewind the buffer stream
      buffer.reset();

      // close original stream - all tokens buffered
      source.close();
    }
    catch (IOException e) {
      // ignore
    }

    if (numTokens == 0)
      return null;
    else if (numTokens == 1) {
      String term = null;
      try {
        boolean hasNext = buffer.incrementToken();
        assert hasNext == true;
        term = termAtt.toString();
      } catch (IOException e) {
        // safe to ignore, because we know the number of tokens
      }
      // return newTermQuery(new Term(field, term));
      return new TermQuery(new Term(field, term));
    } else {
      if (severalTokensAtSamePosition) {
        if (positionCount == 1) {
          // no phrase query:
          // BooleanQuery q = newBooleanQuery(true);
          BooleanQuery q = new BooleanQuery(true);
          for (int i = 0; i < numTokens; i++) {
            String term = null;
            try {
              boolean hasNext = buffer.incrementToken();
              assert hasNext == true;
              term = termAtt.toString();
            } catch (IOException e) {
              // safe to ignore, because we know the number of tokens
            }

            // Query currentQuery = newTermQuery(new Term(field, term));
            Query currentQuery = new TermQuery(new Term(field, term));
            q.add(currentQuery, BooleanClause.Occur.SHOULD);
          }
          return q;
        }
        else {
          // phrase query:
          // MultiPhraseQuery mpq = newMultiPhraseQuery();
          MultiPhraseQuery mpq = new MultiPhraseQuery();
          mpq.setSlop(phraseSlop);
          List multiTerms = new ArrayList();
          int position = -1;
          for (int i = 0; i < numTokens; i++) {
            String term = null;
            int positionIncrement = 1;
            try {
              boolean hasNext = buffer.incrementToken();
              assert hasNext == true;
              term = termAtt.toString();
              if (posIncrAtt != null) {
                positionIncrement = posIncrAtt.getPositionIncrement();
              }
            } catch (IOException e) {
              // safe to ignore, because we know the number of tokens
            }

            if (positionIncrement > 0 && multiTerms.size() > 0) {
              if (enablePositionIncrements) {
                mpq.add((Term[])multiTerms.toArray(new Term[0]),position);
              } else {
                mpq.add((Term[])multiTerms.toArray(new Term[0]));
              }
              multiTerms.clear();
            }
            position += positionIncrement;
            multiTerms.add(new Term(field, term));
          }
          if (enablePositionIncrements) {
            mpq.add((Term[])multiTerms.toArray(new Term[0]),position);
          } else {
            mpq.add((Term[])multiTerms.toArray(new Term[0]));
          }
          return mpq;
        }
      }
      else {
        // PhraseQuery pq = newPhraseQuery();
        PhraseQuery pq = new PhraseQuery();
        pq.setSlop(phraseSlop);
        int position = -1;


        for (int i = 0; i < numTokens; i++) {
          String term = null;
          int positionIncrement = 1;

          try {
            boolean hasNext = buffer.incrementToken();
            assert hasNext == true;
            term = termAtt.toString();
            if (posIncrAtt != null) {
              positionIncrement = posIncrAtt.getPositionIncrement();
            }
          } catch (IOException e) {
            // safe to ignore, because we know the number of tokens
          }

          if (enablePositionIncrements) {
            position += positionIncrement;
            pq.add(new Term(field, term),position);
          } else {
            pq.add(new Term(field, term));
          }
        }
        return pq;
      }
    }

  }

}
