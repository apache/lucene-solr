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
package org.apache.lucene.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostAttribute;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanBoostQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.graph.GraphTokenStreamFiniteStrings;

import static org.apache.lucene.search.BoostAttribute.DEFAULT_BOOST;

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
  protected Analyzer analyzer;
  protected boolean enablePositionIncrements = true;
  protected boolean enableGraphQueries = true;
  protected boolean autoGenerateMultiTermSynonymsPhraseQuery = false;

  /**
   * Wraps a term and boost
   */
  public static class TermAndBoost {
    /** the term */
    public final Term term;
    /** the boost */
    public final float boost;

    /**
     * Creates a new TermAndBoost
     */
    public TermAndBoost(Term term, float boost) {
      this.term = term;
      this.boost = boost;
    }
  }

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
    
    // TODO: weird that BQ equals/rewrite/scorer doesn't handle this?
    if (fraction == 1) {
      return createBooleanQuery(field, queryText, BooleanClause.Occur.MUST);
    }
    
    Query query = createFieldQuery(analyzer, BooleanClause.Occur.SHOULD, field, queryText, false, 0);
    if (query instanceof BooleanQuery) {
      query = addMinShouldMatchToBoolean((BooleanQuery) query, fraction);
    }
    return query;
  }

  /**
   * Rebuilds a boolean query and sets a new minimum number should match value.
   */
  private BooleanQuery addMinShouldMatchToBoolean(BooleanQuery query, float fraction) {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.setMinimumNumberShouldMatch((int) (fraction * query.clauses().size()));
    for (BooleanClause clause : query) {
      builder.add(clause);
    }

    return builder.build();
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
   * Returns true if phrase query should be automatically generated for multi terms synonyms.
   * @see #setAutoGenerateMultiTermSynonymsPhraseQuery(boolean)
   */
  public boolean getAutoGenerateMultiTermSynonymsPhraseQuery() {
    return autoGenerateMultiTermSynonymsPhraseQuery;
  }

  /**
   * Set to <code>true</code> if phrase queries should be automatically generated
   * for multi terms synonyms.
   * Default: false.
   */
  public void setAutoGenerateMultiTermSynonymsPhraseQuery(boolean enable) {
    this.autoGenerateMultiTermSynonymsPhraseQuery = enable;
  }

  /**
   * Creates a query from the analysis chain.
   * <p>
   * Expert: this is more useful for subclasses such as queryparsers.
   * If using this class directly, just use {@link #createBooleanQuery(String, String)}
   * and {@link #createPhraseQuery(String, String)}.  This is a complex method and
   * it is usually not necessary to override it in a subclass; instead, override
   * methods like {@link #newBooleanQuery}, etc., if possible.
   *
   * @param analyzer   analyzer used for this query
   * @param operator   default boolean operator used for this query
   * @param field      field to create queries against
   * @param queryText  text to be passed to the analysis chain
   * @param quoted     true if phrases should be generated when terms occur at more than one position
   * @param phraseSlop slop factor for phrase/multiphrase queries
   */
  protected Query createFieldQuery(Analyzer analyzer, BooleanClause.Occur operator, String field, String queryText, boolean quoted, int phraseSlop) {
    assert operator == BooleanClause.Occur.SHOULD || operator == BooleanClause.Occur.MUST;

    // Use the analyzer to get all the tokens, and then build an appropriate
    // query based on the analysis chain.
    try (TokenStream source = analyzer.tokenStream(field, queryText)) {
      return createFieldQuery(source, operator, field, quoted, phraseSlop);
    } catch (IOException e) {
      throw new RuntimeException("Error analyzing query text", e);
    }
  }

  /** Enable or disable graph TokenStream processing (enabled by default).
   *
   * @lucene.experimental */
  public void setEnableGraphQueries(boolean v) {
    enableGraphQueries = v;
  }

  /** Returns true if graph TokenStream processing is enabled (default).
   *
   * @lucene.experimental */
  public boolean getEnableGraphQueries() {
    return enableGraphQueries;
  }

  /**
   * Creates a query from a token stream.
   *
   * @param source     the token stream to create the query from
   * @param operator   default boolean operator used for this query
   * @param field      field to create queries against
   * @param quoted     true if phrases should be generated when terms occur at more than one position
   * @param phraseSlop slop factor for phrase/multiphrase queries
   */
  protected Query createFieldQuery(TokenStream source, BooleanClause.Occur operator, String field, boolean quoted, int phraseSlop) {
    assert operator == BooleanClause.Occur.SHOULD || operator == BooleanClause.Occur.MUST;

    // Build an appropriate query based on the analysis chain.
    try (CachingTokenFilter stream = new CachingTokenFilter(source)) {
      
      TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
      PositionIncrementAttribute posIncAtt = stream.addAttribute(PositionIncrementAttribute.class);
      PositionLengthAttribute posLenAtt = stream.addAttribute(PositionLengthAttribute.class);

      if (termAtt == null) {
        return null; 
      }
      
      // phase 1: read through the stream and assess the situation:
      // counting the number of tokens/positions and marking if we have any synonyms.
      
      int numTokens = 0;
      int positionCount = 0;
      boolean hasSynonyms = false;
      boolean isGraph = false;

      stream.reset();
      while (stream.incrementToken()) {
        numTokens++;
        int positionIncrement = posIncAtt.getPositionIncrement();
        if (positionIncrement != 0) {
          positionCount += positionIncrement;
        } else {
          hasSynonyms = true;
        }

        int positionLength = posLenAtt.getPositionLength();
        if (enableGraphQueries && positionLength > 1) {
          isGraph = true;
        }
      }
      
      // phase 2: based on token count, presence of synonyms, and options
      // formulate a single term, boolean, or phrase.
      
      if (numTokens == 0) {
        return null;
      } else if (numTokens == 1) {
        // single term
        return analyzeTerm(field, stream);
      } else if (isGraph) {
        // graph
        if (quoted) {
          return analyzeGraphPhrase(stream, field, phraseSlop);
        } else {
          return analyzeGraphBoolean(field, stream, operator);
        }
      } else if (quoted && positionCount > 1) {
        // phrase
        if (hasSynonyms) {
          // complex phrase with synonyms
          return analyzeMultiPhrase(field, stream, phraseSlop);
        } else {
          // simple phrase
          return analyzePhrase(field, stream, phraseSlop);
        }
      } else {
        // boolean
        if (positionCount == 1) {
          // only one position, with synonyms
          return analyzeBoolean(field, stream);
        } else {
          // complex case: multiple positions
          return analyzeMultiBoolean(field, stream, operator);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Error analyzing query text", e);
    }
  }

  /**
   * Creates a span query from the tokenstream.  In the case of a single token, a simple <code>SpanTermQuery</code> is
   * returned.  When multiple tokens, an ordered <code>SpanNearQuery</code> with slop 0 is returned.
   */
  protected SpanQuery createSpanQuery(TokenStream in, String field) throws IOException {
    TermToBytesRefAttribute termAtt = in.getAttribute(TermToBytesRefAttribute.class);
    BoostAttribute boostAtt = in.addAttribute(BoostAttribute.class);

    SpanQuery result;
    float boost = DEFAULT_BOOST;
    if (termAtt == null) {
      return null;
    }

    List<SpanTermQuery> terms = new ArrayList<>();
    while (in.incrementToken()) {
      boost *= boostAtt.getBoost();
      terms.add(new SpanTermQuery(new Term(field, termAtt.getBytesRef())));
    }

    if (terms.isEmpty()) {
      return null;
    } else if (terms.size() == 1) {
      result = terms.get(0);
    } else {
      result = new SpanNearQuery(terms.toArray(new SpanQuery[0]), 0, true);
    }

    if (boost != DEFAULT_BOOST) {
      result = new SpanBoostQuery(result, boost);
    }
    return result;
  }

  /** 
   * Creates simple term query from the cached tokenstream contents 
   */
  protected Query analyzeTerm(String field, TokenStream stream) throws IOException {
    TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
    BoostAttribute boostAtt = stream.addAttribute(BoostAttribute.class);
    
    stream.reset();
    if (!stream.incrementToken()) {
      throw new AssertionError();
    }
    
    return newTermQuery(new Term(field, termAtt.getBytesRef()), boostAtt.getBoost());
  }
  
  /** 
   * Creates simple boolean query from the cached tokenstream contents 
   */
  protected Query analyzeBoolean(String field, TokenStream stream) throws IOException {
    TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
    BoostAttribute boostAtt = stream.addAttribute(BoostAttribute.class);
    
    stream.reset();
    List<TermAndBoost> terms = new ArrayList<>();
    while (stream.incrementToken()) {
      terms.add(new TermAndBoost(new Term(field, termAtt.getBytesRef()), boostAtt.getBoost()));
    }
    
    return newSynonymQuery(terms.toArray(new TermAndBoost[0]));
  }

  protected void add(BooleanQuery.Builder q, List<TermAndBoost> current, BooleanClause.Occur operator) {
    if (current.isEmpty()) {
      return;
    }
    if (current.size() == 1) {
      q.add(newTermQuery(current.get(0).term, current.get(0).boost), operator);
    } else {
      q.add(newSynonymQuery(current.toArray(new TermAndBoost[0])), operator);
    }
  }

  /** 
   * Creates complex boolean query from the cached tokenstream contents 
   */
  protected Query analyzeMultiBoolean(String field, TokenStream stream, BooleanClause.Occur operator) throws IOException {
    BooleanQuery.Builder q = newBooleanQuery();
    List<TermAndBoost> currentQuery = new ArrayList<>();
    
    TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
    PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
    BoostAttribute boostAtt = stream.addAttribute(BoostAttribute.class);

    stream.reset();
    while (stream.incrementToken()) {
      if (posIncrAtt.getPositionIncrement() != 0) {
        add(q, currentQuery, operator);
        currentQuery.clear();
      }
      currentQuery.add(new TermAndBoost(new Term(field, termAtt.getBytesRef()), boostAtt.getBoost()));
    }
    add(q, currentQuery, operator);
    
    return q.build();
  }
  
  /** 
   * Creates simple phrase query from the cached tokenstream contents 
   */
  protected Query analyzePhrase(String field, TokenStream stream, int slop) throws IOException {
    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    builder.setSlop(slop);
    
    TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
    BoostAttribute boostAtt = stream.addAttribute(BoostAttribute.class);
    PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
    int position = -1;
    float phraseBoost = DEFAULT_BOOST;
    stream.reset();
    while (stream.incrementToken()) {
      if (enablePositionIncrements) {
        position += posIncrAtt.getPositionIncrement();
      } else {
        position += 1;
      }
      builder.add(new Term(field, termAtt.getBytesRef()), position);
      phraseBoost *= boostAtt.getBoost();
    }
    PhraseQuery query = builder.build();
    if (phraseBoost == DEFAULT_BOOST) {
      return query;
    }
    return new BoostQuery(query, phraseBoost);
  }
  
  /** 
   * Creates complex phrase query from the cached tokenstream contents 
   */
  protected Query analyzeMultiPhrase(String field, TokenStream stream, int slop) throws IOException {
    MultiPhraseQuery.Builder mpqb = newMultiPhraseQueryBuilder();
    mpqb.setSlop(slop);
    
    TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);

    PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
    int position = -1;  
    
    List<Term> multiTerms = new ArrayList<>();
    stream.reset();
    while (stream.incrementToken()) {
      int positionIncrement = posIncrAtt.getPositionIncrement();
      
      if (positionIncrement > 0 && multiTerms.size() > 0) {
        if (enablePositionIncrements) {
          mpqb.add(multiTerms.toArray(new Term[0]), position);
        } else {
          mpqb.add(multiTerms.toArray(new Term[0]));
        }
        multiTerms.clear();
      }
      position += positionIncrement;
      multiTerms.add(new Term(field, termAtt.getBytesRef()));
    }
    
    if (enablePositionIncrements) {
      mpqb.add(multiTerms.toArray(new Term[0]), position);
    } else {
      mpqb.add(multiTerms.toArray(new Term[0]));
    }
    return mpqb.build();
  }

  /**
   * Creates a boolean query from a graph token stream. The articulation points of the graph are visited in order and the queries
   * created at each point are merged in the returned boolean query.
   */
  protected Query analyzeGraphBoolean(String field, TokenStream source, BooleanClause.Occur operator) throws IOException {
    source.reset();
    GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(source);
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    int[] articulationPoints = graph.articulationPoints();
    int lastState = 0;
    for (int i = 0; i <= articulationPoints.length; i++) {
      int start = lastState;
      int end = -1;
      if (i < articulationPoints.length) {
        end = articulationPoints[i];
      }
      lastState = end;
      final Query positionalQuery;
      if (graph.hasSidePath(start)) {
        final Iterator<TokenStream> sidePathsIterator = graph.getFiniteStrings(start, end);
        Iterator<Query> queries = new Iterator<Query>() {
          @Override
          public boolean hasNext() {
            return sidePathsIterator.hasNext();
          }

          @Override
          public Query next() {
            TokenStream sidePath = sidePathsIterator.next();
            return createFieldQuery(sidePath, BooleanClause.Occur.MUST, field, getAutoGenerateMultiTermSynonymsPhraseQuery(), 0);
          }
        };
        positionalQuery = newGraphSynonymQuery(queries);
      } else {
        List<AttributeSource> attributes = graph.getTerms(start);
        TermAndBoost[] terms = attributes.stream()
                .map(s -> {
                  TermToBytesRefAttribute t = s.addAttribute(TermToBytesRefAttribute.class);
                  BoostAttribute b = s.addAttribute(BoostAttribute.class);
                  return new TermAndBoost(new Term(field, t.getBytesRef()), b.getBoost());
                })
                .toArray(TermAndBoost[]::new);
        assert terms.length > 0;
        if (terms.length == 1) {
          positionalQuery = newTermQuery(terms[0].term, terms[0].boost);
        } else {
          positionalQuery = newSynonymQuery(terms);
        }
      }
      if (positionalQuery != null) {
        builder.add(positionalQuery, operator);
      }
    }
    return builder.build();
  }

  /**
   * Creates graph phrase query from the tokenstream contents
   */
  protected Query analyzeGraphPhrase(TokenStream source, String field, int phraseSlop)
      throws IOException {
    source.reset();
    GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(source);
    if (phraseSlop > 0) {
      /**
       * Creates a boolean query from the graph token stream by extracting all the finite strings from the graph
       * and using them to create phrase queries with the appropriate slop.
       */
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      Iterator<TokenStream> it = graph.getFiniteStrings();
      while (it.hasNext()) {
        Query query = createFieldQuery(it.next(), BooleanClause.Occur.MUST, field, true, phraseSlop);
        if (query != null) {
          builder.add(query, BooleanClause.Occur.SHOULD);
        }
      }
      return builder.build();
    }

    /**
     * Creates a span near (phrase) query from a graph token stream.
     * The articulation points of the graph are visited in order and the queries
     * created at each point are merged in the returned near query.
     */
    List<SpanQuery> clauses = new ArrayList<>();
    int[] articulationPoints = graph.articulationPoints();
    int lastState = 0;
    int maxClauseCount = BooleanQuery.getMaxClauseCount();
    for (int i = 0; i <= articulationPoints.length; i++) {
      int start = lastState;
      int end = -1;
      if (i < articulationPoints.length) {
        end = articulationPoints[i];
      }
      lastState = end;
      final SpanQuery queryPos;
      if (graph.hasSidePath(start)) {
        List<SpanQuery> queries = new ArrayList<>();
        Iterator<TokenStream> it = graph.getFiniteStrings(start, end);
        while (it.hasNext()) {
          TokenStream ts = it.next();
          SpanQuery q = createSpanQuery(ts, field);
          if (q != null) {
            if (queries.size() >= maxClauseCount) {
              throw new BooleanQuery.TooManyClauses();
            }
            queries.add(q);
          }
        }
        if (queries.size() > 0) {
          queryPos = new SpanOrQuery(queries.toArray(new SpanQuery[0]));
        } else {
          queryPos = null;
        }
      } else {
        Term[] terms = graph.getTerms(field, start);
        assert terms.length > 0;
        if (terms.length == 1) {
          queryPos = new SpanTermQuery(terms[0]);
        } else {
          if (terms.length >= maxClauseCount) {
            throw new BooleanQuery.TooManyClauses();
          }
          SpanTermQuery[] orClauses = new SpanTermQuery[terms.length];
          for (int idx = 0; idx < terms.length; idx++) {
            orClauses[idx] = new SpanTermQuery(terms[idx]);
          }

          queryPos = new SpanOrQuery(orClauses);
        }
      }

      if (queryPos != null) {
        if (clauses.size() >= maxClauseCount) {
          throw new BooleanQuery.TooManyClauses();
        }
        clauses.add(queryPos);
      }
    }

    if (clauses.isEmpty()) {
      return null;
    } else if (clauses.size() == 1) {
      return clauses.get(0);
    } else {
      return new SpanNearQuery(clauses.toArray(new SpanQuery[0]), 0, true);
    }
  }

  /**
   * Builds a new BooleanQuery instance.
   * <p>
   * This is intended for subclasses that wish to customize the generated queries.
   * @return new BooleanQuery instance
   */
  protected BooleanQuery.Builder newBooleanQuery() {
    return new BooleanQuery.Builder();
  }
  
  /**
   * Builds a new SynonymQuery instance.
   * <p>
   * This is intended for subclasses that wish to customize the generated queries.
   * @return new Query instance
   */
  protected Query newSynonymQuery(TermAndBoost[] terms) {
    SynonymQuery.Builder builder = new SynonymQuery.Builder(terms[0].term.field());
    for (TermAndBoost t : terms) {
      builder.addTerm(t.term, t.boost);
    }
    return builder.build();
  }

  /**
   * Builds a new GraphQuery for multi-terms synonyms.
   * <p>
   * This is intended for subclasses that wish to customize the generated queries.
   * @return new Query instance
   */
  protected Query newGraphSynonymQuery(Iterator<Query> queries) {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    while (queries.hasNext()) {
      builder.add(queries.next(), BooleanClause.Occur.SHOULD);
    }
    BooleanQuery bq = builder.build();
    if (bq.clauses().size() == 1) {
      return bq.clauses().get(0).getQuery();
    }
    return bq;
  }
  
  /**
   * Builds a new TermQuery instance.
   * <p>
   * This is intended for subclasses that wish to customize the generated queries.
   * @param term term
   * @return new TermQuery instance
   */
  protected Query newTermQuery(Term term, float boost) {
    Query q = new TermQuery(term);
    if (boost == DEFAULT_BOOST) {
      return q;
    }
    return new BoostQuery(q, boost);
  }
  
  
  /**
   * Builds a new MultiPhraseQuery instance.
   * <p>
   * This is intended for subclasses that wish to customize the generated queries.
   * @return new MultiPhraseQuery instance
   */
  protected MultiPhraseQuery.Builder newMultiPhraseQueryBuilder() {
    return new MultiPhraseQuery.Builder();
  }
}
