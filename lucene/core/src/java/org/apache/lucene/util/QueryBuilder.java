package org.apache.lucene.util;

/*
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

/**
 * Creates queries from the {@link Analyzer} chain.
 * <p>
 * Example usage:
 * <pre class="prettyprint">
 *   QueryBuilder builder = new QueryBuilder(analyzer);
 *   Query a = builder.createBooleanQuery("body", "just a test");
 *   Query b = builder.createPhraseQuery("body", "another test");
 *   Query c = builder.createMinShouldMatchQuery("body", "another test", 0.5f);
 * </pre>
 * <p>
 * This can also be used as a subclass for query parsers to make it easier
 * to interact with the analysis chain. Factory methods such as {@code newTermQuery} 
 * are provided so that the generated queries can be customized.
 */
public class QueryBuilder {
  private Analyzer analyzer;
  private boolean enablePositionIncrements = true;
  
  /** Creates a new QueryBuilder using the given analyzer. */
  public QueryBuilder(Analyzer analyzer) {
    this.analyzer = analyzer;
  }
  
  /** 
   * Creates a boolean query from the query text.
   * <p>
   * This is equivalent to {@code createBooleanQuery(field, queryText, Occur.SHOULD)}
   * @param field field name
   * @param queryText text to be passed to the analyzer
   * @return {@code TermQuery} or {@code BooleanQuery}, based on the analysis
   *         of {@code queryText}
   */
  public Query createBooleanQuery(String field, String queryText) {
    return createBooleanQuery(field, queryText, BooleanClause.Occur.SHOULD);
  }
  
  /** 
   * Creates a boolean query from the query text.
   * <p>
   * @param field field name
   * @param queryText text to be passed to the analyzer
   * @param operator operator used for clauses between analyzer tokens.
   * @return {@code TermQuery} or {@code BooleanQuery}, based on the analysis 
   *         of {@code queryText}
   */
  public Query createBooleanQuery(String field, String queryText, BooleanClause.Occur operator) {
    if (operator != BooleanClause.Occur.SHOULD && operator != BooleanClause.Occur.MUST) {
      throw new IllegalArgumentException("invalid operator: only SHOULD or MUST are allowed");
    }
    return createFieldQuery(analyzer, operator, field, queryText, false, 0);
  }
  
  /** 
   * Creates a phrase query from the query text.
   * <p>
   * This is equivalent to {@code createPhraseQuery(field, queryText, 0)}
   * @param field field name
   * @param queryText text to be passed to the analyzer
   * @return {@code TermQuery}, {@code BooleanQuery}, {@code PhraseQuery}, or
   *         {@code MultiPhraseQuery}, based on the analysis of {@code queryText}
   */
  public Query createPhraseQuery(String field, String queryText) {
    return createPhraseQuery(field, queryText, 0);
  }
  
  /** 
   * Creates a phrase query from the query text.
   * <p>
   * @param field field name
   * @param queryText text to be passed to the analyzer
   * @param phraseSlop number of other words permitted between words in query phrase
   * @return {@code TermQuery}, {@code BooleanQuery}, {@code PhraseQuery}, or
   *         {@code MultiPhraseQuery}, based on the analysis of {@code queryText}
   */
  public Query createPhraseQuery(String field, String queryText, int phraseSlop) {
    return createFieldQuery(analyzer, BooleanClause.Occur.MUST, field, queryText, true, phraseSlop);
  }
  
  /** 
   * Creates a minimum-should-match query from the query text.
   * <p>
   * @param field field name
   * @param queryText text to be passed to the analyzer
   * @param fraction of query terms {@code [0..1]} that should match 
   * @return {@code TermQuery} or {@code BooleanQuery}, based on the analysis 
   *         of {@code queryText}
   */
  public Query createMinShouldMatchQuery(String field, String queryText, float fraction) {
    if (Float.isNaN(fraction) || fraction < 0 || fraction > 1) {
      throw new IllegalArgumentException("fraction should be >= 0 and <= 1");
    }
    
    // TODO: wierd that BQ equals/rewrite/scorer doesn't handle this?
    if (fraction == 1) {
      return createBooleanQuery(field, queryText, BooleanClause.Occur.MUST);
    }
    
    Query query = createFieldQuery(analyzer, BooleanClause.Occur.SHOULD, field, queryText, false, 0);
    if (query instanceof BooleanQuery) {
      BooleanQuery bq = (BooleanQuery) query;
      bq.setMinimumNumberShouldMatch((int) (fraction * bq.clauses().size()));
    }
    return query;
  }
  
  /** 
   * Returns the analyzer. 
   * @see #setAnalyzer(Analyzer)
   */
  public Analyzer getAnalyzer() {
    return analyzer;
  }
  
  /** 
   * Sets the analyzer used to tokenize text.
   */
  public void setAnalyzer(Analyzer analyzer) {
    this.analyzer = analyzer;
  }
  
  /**
   * Returns true if position increments are enabled.
   * @see #setEnablePositionIncrements(boolean)
   */
  public boolean getEnablePositionIncrements() {
    return enablePositionIncrements;
  }
  
  /**
   * Set to <code>true</code> to enable position increments in result query.
   * <p>
   * When set, result phrase and multi-phrase queries will
   * be aware of position increments.
   * Useful when e.g. a StopFilter increases the position increment of
   * the token that follows an omitted token.
   * <p>
   * Default: true.
   */
  public void setEnablePositionIncrements(boolean enable) {
    this.enablePositionIncrements = enable;
  }

  /**
   * Creates a query from the analysis chain.
   * <p>
   * Expert: this is more useful for subclasses such as queryparsers. 
   * If using this class directly, just use {@link #createBooleanQuery(String, String)}
   * and {@link #createPhraseQuery(String, String)}
   * @param analyzer analyzer used for this query
   * @param operator default boolean operator used for this query
   * @param field field to create queries against
   * @param queryText text to be passed to the analysis chain
   * @param quoted true if phrases should be generated when terms occur at more than one position
   * @param phraseSlop slop factor for phrase/multiphrase queries
   */
  protected final Query createFieldQuery(Analyzer analyzer, BooleanClause.Occur operator, String field, String queryText, boolean quoted, int phraseSlop) {
    assert operator == BooleanClause.Occur.SHOULD || operator == BooleanClause.Occur.MUST;
    // Use the analyzer to get all the tokens, and then build a TermQuery,
    // PhraseQuery, or nothing based on the term count
    CachingTokenFilter buffer = null;
    TermToBytesRefAttribute termAtt = null;
    PositionIncrementAttribute posIncrAtt = null;
    int numTokens = 0;
    int positionCount = 0;
    boolean severalTokensAtSamePosition = false;
    boolean hasMoreTokens = false;

    try (TokenStream source = analyzer.tokenStream(field, queryText)) {
      buffer = new CachingTokenFilter(source);
      buffer.reset();

      termAtt = buffer.getAttribute(TermToBytesRefAttribute.class);
      posIncrAtt = buffer.getAttribute(PositionIncrementAttribute.class);

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
    } catch (IOException e) {
      throw new RuntimeException("Error analyzing query text", e);
    }

    // rewind the buffer stream
    try {
      if (numTokens > 0) {
        buffer.reset();//will never throw; the buffer is cached
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    BytesRef bytes = termAtt == null ? null : termAtt.getBytesRef();

    if (numTokens == 0) {
      return null;
    } else if (numTokens == 1) {
      try {
        boolean hasNext = buffer.incrementToken();
        assert hasNext == true;
        termAtt.fillBytesRef();
      } catch (IOException e) {
        // safe to ignore, because we know the number of tokens
      }
      return newTermQuery(new Term(field, BytesRef.deepCopyOf(bytes)));
    } else {
      if (severalTokensAtSamePosition || (!quoted)) {
        if (positionCount == 1 || (!quoted)) {
          // no phrase query:
          
          if (positionCount == 1) {
            // simple case: only one position, with synonyms
            BooleanQuery q = newBooleanQuery(true);
            for (int i = 0; i < numTokens; i++) {
              try {
                boolean hasNext = buffer.incrementToken();
                assert hasNext == true;
                termAtt.fillBytesRef();
              } catch (IOException e) {
                // safe to ignore, because we know the number of tokens
              }
              Query currentQuery = newTermQuery(
                  new Term(field, BytesRef.deepCopyOf(bytes)));
              q.add(currentQuery, BooleanClause.Occur.SHOULD);
            }
            return q;
          } else {
            // multiple positions
            BooleanQuery q = newBooleanQuery(false);
            Query currentQuery = null;
            for (int i = 0; i < numTokens; i++) {
              try {
                boolean hasNext = buffer.incrementToken();
                assert hasNext == true;
                termAtt.fillBytesRef();
              } catch (IOException e) {
                // safe to ignore, because we know the number of tokens
              }
              if (posIncrAtt != null && posIncrAtt.getPositionIncrement() == 0) {
                if (!(currentQuery instanceof BooleanQuery)) {
                  Query t = currentQuery;
                  currentQuery = newBooleanQuery(true);
                  ((BooleanQuery)currentQuery).add(t, BooleanClause.Occur.SHOULD);
                }
                ((BooleanQuery)currentQuery).add(newTermQuery(new Term(field, BytesRef.deepCopyOf(bytes))), BooleanClause.Occur.SHOULD);
              } else {
                if (currentQuery != null) {
                  q.add(currentQuery, operator);
                }
                currentQuery = newTermQuery(new Term(field, BytesRef.deepCopyOf(bytes)));
              }
            }
            q.add(currentQuery, operator);
            return q;
          }
        } else {
          // phrase query:
          MultiPhraseQuery mpq = newMultiPhraseQuery();
          mpq.setSlop(phraseSlop);
          List<Term> multiTerms = new ArrayList<>();
          int position = -1;
          for (int i = 0; i < numTokens; i++) {
            int positionIncrement = 1;
            try {
              boolean hasNext = buffer.incrementToken();
              assert hasNext == true;
              termAtt.fillBytesRef();
              if (posIncrAtt != null) {
                positionIncrement = posIncrAtt.getPositionIncrement();
              }
            } catch (IOException e) {
              // safe to ignore, because we know the number of tokens
            }

            if (positionIncrement > 0 && multiTerms.size() > 0) {
              if (enablePositionIncrements) {
                mpq.add(multiTerms.toArray(new Term[0]),position);
              } else {
                mpq.add(multiTerms.toArray(new Term[0]));
              }
              multiTerms.clear();
            }
            position += positionIncrement;
            multiTerms.add(new Term(field, BytesRef.deepCopyOf(bytes)));
          }
          if (enablePositionIncrements) {
            mpq.add(multiTerms.toArray(new Term[0]),position);
          } else {
            mpq.add(multiTerms.toArray(new Term[0]));
          }
          return mpq;
        }
      } else {
        PhraseQuery pq = newPhraseQuery();
        pq.setSlop(phraseSlop);
        int position = -1;

        for (int i = 0; i < numTokens; i++) {
          int positionIncrement = 1;

          try {
            boolean hasNext = buffer.incrementToken();
            assert hasNext == true;
            termAtt.fillBytesRef();
            if (posIncrAtt != null) {
              positionIncrement = posIncrAtt.getPositionIncrement();
            }
          } catch (IOException e) {
            // safe to ignore, because we know the number of tokens
          }

          if (enablePositionIncrements) {
            position += positionIncrement;
            pq.add(new Term(field, BytesRef.deepCopyOf(bytes)),position);
          } else {
            pq.add(new Term(field, BytesRef.deepCopyOf(bytes)));
          }
        }
        return pq;
      }
    }
  }
  
  /**
   * Builds a new BooleanQuery instance.
   * <p>
   * This is intended for subclasses that wish to customize the generated queries.
   * @param disableCoord disable coord
   * @return new BooleanQuery instance
   */
  protected BooleanQuery newBooleanQuery(boolean disableCoord) {
    return new BooleanQuery(disableCoord);
  }
  
  /**
   * Builds a new TermQuery instance.
   * <p>
   * This is intended for subclasses that wish to customize the generated queries.
   * @param term term
   * @return new TermQuery instance
   */
  protected Query newTermQuery(Term term) {
    return new TermQuery(term);
  }
  
  /**
   * Builds a new PhraseQuery instance.
   * <p>
   * This is intended for subclasses that wish to customize the generated queries.
   * @return new PhraseQuery instance
   */
  protected PhraseQuery newPhraseQuery() {
    return new PhraseQuery();
  }
  
  /**
   * Builds a new MultiPhraseQuery instance.
   * <p>
   * This is intended for subclasses that wish to customize the generated queries.
   * @return new MultiPhraseQuery instance
   */
  protected MultiPhraseQuery newMultiPhraseQuery() {
    return new MultiPhraseQuery();
  }
}
