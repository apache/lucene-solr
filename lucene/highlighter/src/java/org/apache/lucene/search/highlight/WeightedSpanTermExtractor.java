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
package org.apache.lucene.search.highlight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.FieldMaskingSpanQuery;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;

/**
 * Class used to extract {@link WeightedSpanTerm}s from a {@link Query} based on whether 
 * {@link Term}s from the {@link Query} are contained in a supplied {@link TokenStream}.
 *
 * In order to support additional, by default unsupported queries, subclasses can override
 * {@link #extract(Query, float, Map)} for extracting wrapped or delegate queries and
 * {@link #extractUnknownQuery(Query, Map)} to process custom leaf queries:
 * <pre>
 * <code>
 *    WeightedSpanTermExtractor extractor = new WeightedSpanTermExtractor() {
 *        protected void extract(Query query, float boost, Map&lt;String, WeightedSpanTerm&gt;terms) throws IOException {
 *          if (query instanceof QueryWrapper) {
 *            extract(((QueryWrapper)query).getQuery(), boost, terms);
 *          } else {
 *            super.extract(query, boost, terms);
 *          }
 *        }
 *
 *        protected void extractUnknownQuery(Query query, Map&lt;String, WeightedSpanTerm&gt; terms) throws IOException {
 *          if (query instanceOf CustomTermQuery) {
 *            Term term = ((CustomTermQuery) query).getTerm();
 *            terms.put(term.field(), new WeightedSpanTerm(1, term.text()));
 *          }
 *        }
 *    };
 * }
 * </code>
 * </pre>
 */
public class WeightedSpanTermExtractor {

  private String fieldName;
  private TokenStream tokenStream;//set subsequent to getWeightedSpanTerms* methods
  private String defaultField;
  private boolean expandMultiTermQuery;
  private boolean cachedTokenStream;
  private boolean wrapToCaching = true;
  private int maxDocCharsToAnalyze;
  private boolean usePayloads = false;
  private LeafReader internalReader = null;

  public WeightedSpanTermExtractor() {
    this(null);
  }

  public WeightedSpanTermExtractor(String defaultField) {
    this.defaultField = defaultField;
  }

  /**
   * Fills a <code>Map</code> with {@link WeightedSpanTerm}s using the terms from the supplied <code>Query</code>.
   * 
   * @param query
   *          Query to extract Terms from
   * @param terms
   *          Map to place created WeightedSpanTerms in
   * @throws IOException If there is a low-level I/O error
   */
  protected void extract(Query query, float boost, Map<String,WeightedSpanTerm> terms) throws IOException {
    if (query instanceof BoostQuery) {
      BoostQuery boostQuery = (BoostQuery) query;
      extract(boostQuery.getQuery(), boost * boostQuery.getBoost(), terms);
    } else if (query instanceof BooleanQuery) {
      for (BooleanClause clause : (BooleanQuery) query) {
        if (!clause.isProhibited()) {
          extract(clause.getQuery(), boost, terms);
        }
      }
    } else if (query instanceof PhraseQuery) {
      PhraseQuery phraseQuery = ((PhraseQuery) query);
      Term[] phraseQueryTerms = phraseQuery.getTerms();
      if (phraseQueryTerms.length == 1) {
        extractWeightedSpanTerms(terms, new SpanTermQuery(phraseQueryTerms[0]), boost);
      } else {
        SpanQuery[] clauses = new SpanQuery[phraseQueryTerms.length];
        for (int i = 0; i < phraseQueryTerms.length; i++) {
          clauses[i] = new SpanTermQuery(phraseQueryTerms[i]);
        }

        // sum position increments beyond 1
        int positionGaps = 0;
        int[] positions = phraseQuery.getPositions();
        if (positions.length >= 2) {
          // positions are in increasing order.   max(0,...) is just a safeguard.
          positionGaps = Math.max(0, positions[positions.length - 1] - positions[0] - positions.length + 1);
        }

        //if original slop is 0 then require inOrder
        boolean inorder = (phraseQuery.getSlop() == 0);

        SpanNearQuery sp = new SpanNearQuery(clauses, phraseQuery.getSlop() + positionGaps, inorder);
        extractWeightedSpanTerms(terms, sp, boost);
      }
    } else if (query instanceof TermQuery || query instanceof SynonymQuery) {
      extractWeightedTerms(terms, query, boost);
    } else if (query instanceof SpanQuery) {
      extractWeightedSpanTerms(terms, (SpanQuery) query, boost);
    } else if (query instanceof ConstantScoreQuery) {
      final Query q = ((ConstantScoreQuery) query).getQuery();
      if (q != null) {
        extract(q, boost, terms);
      }
    } else if (query instanceof CommonTermsQuery) {
      // specialized since rewriting would change the result query 
      // this query is index sensitive.
      extractWeightedTerms(terms, query, boost);
    } else if (query instanceof DisjunctionMaxQuery) {
      for (Query clause : ((DisjunctionMaxQuery) query)) {
        extract(clause, boost, terms);
      }
    } else if (query instanceof MultiPhraseQuery) {
      final MultiPhraseQuery mpq = (MultiPhraseQuery) query;
      final Term[][] termArrays = mpq.getTermArrays();
      final int[] positions = mpq.getPositions();
      if (positions.length > 0) {

        int maxPosition = positions[positions.length - 1];
        for (int i = 0; i < positions.length - 1; ++i) {
          if (positions[i] > maxPosition) {
            maxPosition = positions[i];
          }
        }

        @SuppressWarnings({"unchecked","rawtypes"})
        final List<SpanQuery>[] disjunctLists = new List[maxPosition + 1];
        int distinctPositions = 0;

        for (int i = 0; i < termArrays.length; ++i) {
          final Term[] termArray = termArrays[i];
          List<SpanQuery> disjuncts = disjunctLists[positions[i]];
          if (disjuncts == null) {
            disjuncts = (disjunctLists[positions[i]] = new ArrayList<>(termArray.length));
            ++distinctPositions;
          }
          for (Term aTermArray : termArray) {
            disjuncts.add(new SpanTermQuery(aTermArray));
          }
        }

        int positionGaps = 0;
        int position = 0;
        final SpanQuery[] clauses = new SpanQuery[distinctPositions];
        for (List<SpanQuery> disjuncts : disjunctLists) {
          if (disjuncts != null) {
            clauses[position++] = new SpanOrQuery(disjuncts
                .toArray(new SpanQuery[disjuncts.size()]));
          } else {
            ++positionGaps;
          }
        }

        if (clauses.length == 1) {
          extractWeightedSpanTerms(terms, clauses[0], boost);
        } else {
          final int slop = mpq.getSlop();
          final boolean inorder = (slop == 0);

          SpanNearQuery sp = new SpanNearQuery(clauses, slop + positionGaps, inorder);
          extractWeightedSpanTerms(terms, sp, boost);
        }
      }
    } else if (query instanceof MatchAllDocsQuery) {
      //nothing
    } else if (query instanceof FunctionScoreQuery) {
      extract(((FunctionScoreQuery) query).getWrappedQuery(), boost, terms);
    } else if (isQueryUnsupported(query.getClass())) {
      // nothing
    } else {
      if (query instanceof MultiTermQuery &&
          (!expandMultiTermQuery || !fieldNameComparator(((MultiTermQuery)query).getField()))) {
        return;
      }
      Query origQuery = query;
      final IndexReader reader = getLeafContext().reader();
      Query rewritten;
      if (query instanceof MultiTermQuery) {
        rewritten = MultiTermQuery.SCORING_BOOLEAN_REWRITE.rewrite(reader, (MultiTermQuery) query);
      } else {
        rewritten = origQuery.rewrite(reader);
      }
      if (rewritten != origQuery) {
        // only rewrite once and then flatten again - the rewritten query could have a special treatment
        // if this method is overwritten in a subclass or above in the next recursion
        extract(rewritten, boost, terms);
      } else {
        extractUnknownQuery(query, terms);
      }
    }
  }

  protected boolean isQueryUnsupported(Class<? extends Query> clazz) {
    // spatial queries do not support highlighting:
    if (clazz.getName().startsWith("org.apache.lucene.spatial.")) {
      return true;
    }
    // spatial3d queries are also not supported:
    if (clazz.getName().startsWith("org.apache.lucene.spatial3d.")) {
      return true;
    }
    return false;
  }

  protected void extractUnknownQuery(Query query,
      Map<String, WeightedSpanTerm> terms) throws IOException {
    
    // for sub-classing to extract custom queries
  }

  /**
   * Fills a <code>Map</code> with {@link WeightedSpanTerm}s using the terms from the supplied <code>SpanQuery</code>.
   * 
   * @param terms
   *          Map to place created WeightedSpanTerms in
   * @param spanQuery
   *          SpanQuery to extract Terms from
   * @throws IOException If there is a low-level I/O error
   */
  protected void extractWeightedSpanTerms(Map<String,WeightedSpanTerm> terms, SpanQuery spanQuery, float boost) throws IOException {
    Set<String> fieldNames;

    if (fieldName == null) {
      fieldNames = new HashSet<>();
      collectSpanQueryFields(spanQuery, fieldNames);
    } else {
      fieldNames = new HashSet<>(1);
      fieldNames.add(fieldName);
    }
    // To support the use of the default field name
    if (defaultField != null) {
      fieldNames.add(defaultField);
    }
    
    Map<String, SpanQuery> queries = new HashMap<>();
 
    Set<Term> nonWeightedTerms = new HashSet<>();
    final boolean mustRewriteQuery = mustRewriteQuery(spanQuery);
    final IndexSearcher searcher = new IndexSearcher(getLeafContext());
    searcher.setQueryCache(null);
    if (mustRewriteQuery) {
      final SpanQuery rewrittenQuery = (SpanQuery) searcher.rewrite(spanQuery);
      for (final String field : fieldNames) {
        queries.put(field, rewrittenQuery);
      }
      rewrittenQuery.visit(QueryVisitor.termCollector(nonWeightedTerms));
    } else {
      spanQuery.visit(QueryVisitor.termCollector(nonWeightedTerms));
    }

    List<PositionSpan> spanPositions = new ArrayList<>();

    for (final String field : fieldNames) {
      final SpanQuery q;
      if (mustRewriteQuery) {
        q = queries.get(field);
      } else {
        q = spanQuery;
      }
      LeafReaderContext context = getLeafContext();
      SpanWeight w = (SpanWeight) searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE_NO_SCORES, 1);
      Bits acceptDocs = context.reader().getLiveDocs();
      final Spans spans = w.getSpans(context, SpanWeight.Postings.POSITIONS);
      if (spans == null) {
        return;
      }

      // collect span positions
      while (spans.nextDoc() != Spans.NO_MORE_DOCS) {
        if (acceptDocs != null && acceptDocs.get(spans.docID()) == false) {
          continue;
        }
        while (spans.nextStartPosition() != Spans.NO_MORE_POSITIONS) {
          spanPositions.add(new PositionSpan(spans.startPosition(), spans.endPosition() - 1));
        }
      }
    }

    if (spanPositions.size() == 0) {
      // no spans found
      return;
    }

    for (final Term queryTerm :  nonWeightedTerms) {

      if (fieldNameComparator(queryTerm.field())) {
        WeightedSpanTerm weightedSpanTerm = terms.get(queryTerm.text());

        if (weightedSpanTerm == null) {
          weightedSpanTerm = new WeightedSpanTerm(boost, queryTerm.text());
          weightedSpanTerm.addPositionSpans(spanPositions);
          weightedSpanTerm.positionSensitive = true;
          terms.put(queryTerm.text(), weightedSpanTerm);
        } else {
          if (spanPositions.size() > 0) {
            weightedSpanTerm.addPositionSpans(spanPositions);
          }
        }
      }
    }
  }

  /**
   * Fills a <code>Map</code> with {@link WeightedSpanTerm}s using the terms from the supplied <code>Query</code>.
   * 
   * @param terms
   *          Map to place created WeightedSpanTerms in
   * @param query
   *          Query to extract Terms from
   * @throws IOException If there is a low-level I/O error
   */
  protected void extractWeightedTerms(Map<String,WeightedSpanTerm> terms, Query query, float boost) throws IOException {
    Set<Term> nonWeightedTerms = new HashSet<>();
    final IndexSearcher searcher = new IndexSearcher(getLeafContext());
    searcher.rewrite(query).visit(QueryVisitor.termCollector(nonWeightedTerms));

    for (final Term queryTerm : nonWeightedTerms) {

      if (fieldNameComparator(queryTerm.field())) {
        WeightedSpanTerm weightedSpanTerm = new WeightedSpanTerm(boost, queryTerm.text());
        terms.put(queryTerm.text(), weightedSpanTerm);
      }
    }
  }

  /**
   * Necessary to implement matches for queries against <code>defaultField</code>
   */
  protected boolean fieldNameComparator(String fieldNameToCheck) {
    boolean rv = fieldName == null || fieldName.equals(fieldNameToCheck)
      || (defaultField != null && defaultField.equals(fieldNameToCheck));
    return rv;
  }

  protected LeafReaderContext getLeafContext() throws IOException {
    if (internalReader == null) {
      boolean cacheIt = wrapToCaching && !(tokenStream instanceof CachingTokenFilter);

      // If it's from term vectors, simply wrap the underlying Terms in a reader
      if (tokenStream instanceof TokenStreamFromTermVector) {
        cacheIt = false;
        Terms termVectorTerms = ((TokenStreamFromTermVector) tokenStream).getTermVectorTerms();
        if (termVectorTerms.hasPositions() && termVectorTerms.hasOffsets()) {
          internalReader = new TermVectorLeafReader(DelegatingLeafReader.FIELD_NAME, termVectorTerms);
        }
      }

      // Use MemoryIndex (index/invert this tokenStream now)
      if (internalReader == null) {
        final MemoryIndex indexer = new MemoryIndex(true, usePayloads);//offsets and payloads
        if (cacheIt) {
          assert !cachedTokenStream;
          tokenStream = new CachingTokenFilter(new OffsetLimitTokenFilter(tokenStream, maxDocCharsToAnalyze));
          cachedTokenStream = true;
          indexer.addField(DelegatingLeafReader.FIELD_NAME, tokenStream);
        } else {
          indexer.addField(DelegatingLeafReader.FIELD_NAME,
              new OffsetLimitTokenFilter(tokenStream, maxDocCharsToAnalyze));
        }
        final IndexSearcher searcher = indexer.createSearcher();
        // MEM index has only atomic ctx
        internalReader = ((LeafReaderContext) searcher.getTopReaderContext()).reader();
      }

      //Now wrap it so we always use a common field.
      this.internalReader = new DelegatingLeafReader(internalReader);
    }

    return internalReader.getContext();
  }
  
  /*
   * This reader will just delegate every call to a single field in the wrapped
   * LeafReader. This way we only need to build this field once rather than
   * N-Times
   */
  static final class DelegatingLeafReader extends FilterLeafReader {
    private static final String FIELD_NAME = "shadowed_field";

    DelegatingLeafReader(LeafReader in) {
      super(in);
    }

    @Override
    public FieldInfos getFieldInfos() {
      throw new UnsupportedOperationException();//TODO merge them
    }

    @Override
    public Terms terms(String field) throws IOException {
      return super.terms(DelegatingLeafReader.FIELD_NAME);
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
      return super.getNumericDocValues(FIELD_NAME);
    }
    
    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
      return super.getBinaryDocValues(FIELD_NAME);
    }
    
    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
      return super.getSortedDocValues(FIELD_NAME);
    }
    
    @Override
    public NumericDocValues getNormValues(String field) throws IOException {
      return super.getNormValues(FIELD_NAME);
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return null;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }

  /**
   * Creates a Map of <code>WeightedSpanTerms</code> from the given <code>Query</code> and <code>TokenStream</code>.
   * 
   * @param query that caused hit
   * @param tokenStream of text to be highlighted
   * @return Map containing WeightedSpanTerms
   * @throws IOException If there is a low-level I/O error
   */
  public Map<String,WeightedSpanTerm> getWeightedSpanTerms(Query query, float boost, TokenStream tokenStream)
      throws IOException {
    return getWeightedSpanTerms(query, boost, tokenStream, null);
  }

  /**
   * Creates a Map of <code>WeightedSpanTerms</code> from the given <code>Query</code> and <code>TokenStream</code>.
   * 
   * @param query that caused hit
   * @param tokenStream of text to be highlighted
   * @param fieldName restricts Term's used based on field name
   * @return Map containing WeightedSpanTerms
   * @throws IOException If there is a low-level I/O error
   */
  public Map<String,WeightedSpanTerm> getWeightedSpanTerms(Query query, float boost, TokenStream tokenStream,
      String fieldName) throws IOException {
    this.fieldName = fieldName;

    Map<String,WeightedSpanTerm> terms = new PositionCheckingMap<>();
    this.tokenStream = tokenStream;
    try {
      extract(query, boost, terms);
    } finally {
      IOUtils.close(internalReader);
    }

    return terms;
  }

  /**
   * Creates a Map of <code>WeightedSpanTerms</code> from the given <code>Query</code> and <code>TokenStream</code>. Uses a supplied
   * <code>IndexReader</code> to properly weight terms (for gradient highlighting).
   * 
   * @param query that caused hit
   * @param tokenStream of text to be highlighted
   * @param fieldName restricts Term's used based on field name
   * @param reader to use for scoring
   * @return Map of WeightedSpanTerms with quasi tf/idf scores
   * @throws IOException If there is a low-level I/O error
   */
  public Map<String,WeightedSpanTerm> getWeightedSpanTermsWithScores(Query query, float boost, TokenStream tokenStream, String fieldName,
      IndexReader reader) throws IOException {
    if (fieldName != null) {
      this.fieldName = fieldName;
    } else {
      this.fieldName = null;
    }
    this.tokenStream = tokenStream;

    Map<String,WeightedSpanTerm> terms = new PositionCheckingMap<>();
    extract(query, boost, terms);

    int totalNumDocs = reader.maxDoc();
    Set<String> weightedTerms = terms.keySet();
    Iterator<String> it = weightedTerms.iterator();

    try {
      while (it.hasNext()) {
        WeightedSpanTerm weightedSpanTerm = terms.get(it.next());
        int docFreq = reader.docFreq(new Term(fieldName, weightedSpanTerm.term));
        // IDF algorithm taken from ClassicSimilarity class
        float idf = (float) (Math.log(totalNumDocs / (double) (docFreq + 1)) + 1.0);
        weightedSpanTerm.weight *= idf;
      }
    } finally {
      IOUtils.close(internalReader);
    }

    return terms;
  }

  protected void collectSpanQueryFields(SpanQuery spanQuery, Set<String> fieldNames) {
    if (spanQuery instanceof FieldMaskingSpanQuery) {
      collectSpanQueryFields(((FieldMaskingSpanQuery)spanQuery).getMaskedQuery(), fieldNames);
    } else if (spanQuery instanceof SpanFirstQuery) {
      collectSpanQueryFields(((SpanFirstQuery)spanQuery).getMatch(), fieldNames);
    } else if (spanQuery instanceof SpanNearQuery) {
      for (final SpanQuery clause : ((SpanNearQuery)spanQuery).getClauses()) {
        collectSpanQueryFields(clause, fieldNames);
      }
    } else if (spanQuery instanceof SpanNotQuery) {
      collectSpanQueryFields(((SpanNotQuery)spanQuery).getInclude(), fieldNames);
    } else if (spanQuery instanceof SpanOrQuery) {
      for (final SpanQuery clause : ((SpanOrQuery)spanQuery).getClauses()) {
        collectSpanQueryFields(clause, fieldNames);
      }
    } else {
      fieldNames.add(spanQuery.getField());
    }
  }
  
  protected boolean mustRewriteQuery(SpanQuery spanQuery) {
    if (!expandMultiTermQuery) {
      return false; // Will throw UnsupportedOperationException in case of a SpanRegexQuery.
    } else if (spanQuery instanceof FieldMaskingSpanQuery) {
      return mustRewriteQuery(((FieldMaskingSpanQuery)spanQuery).getMaskedQuery());
    } else if (spanQuery instanceof SpanFirstQuery) {
      return mustRewriteQuery(((SpanFirstQuery)spanQuery).getMatch());
    } else if (spanQuery instanceof SpanNearQuery) {
      for (final SpanQuery clause : ((SpanNearQuery)spanQuery).getClauses()) {
        if (mustRewriteQuery(clause)) {
          return true;
        }
      }
      return false; 
    } else if (spanQuery instanceof SpanNotQuery) {
      SpanNotQuery spanNotQuery = (SpanNotQuery)spanQuery;
      return mustRewriteQuery(spanNotQuery.getInclude()) || mustRewriteQuery(spanNotQuery.getExclude());
    } else if (spanQuery instanceof SpanOrQuery) {
      for (final SpanQuery clause : ((SpanOrQuery)spanQuery).getClauses()) {
        if (mustRewriteQuery(clause)) {
          return true;
        }
      }
      return false; 
    } else if (spanQuery instanceof SpanTermQuery) {
      return false;
    } else {
      return true;
    }
  }
  
  /**
   * This class makes sure that if both position sensitive and insensitive
   * versions of the same term are added, the position insensitive one wins.
   */
  @SuppressWarnings("serial")
  protected static class PositionCheckingMap<K> extends HashMap<K,WeightedSpanTerm> {

    @Override
    public void putAll(Map<? extends K,? extends WeightedSpanTerm> m) {
      for (Map.Entry<? extends K,? extends WeightedSpanTerm> entry : m.entrySet())
        this.put(entry.getKey(), entry.getValue());
    }

    @Override
    public WeightedSpanTerm put(K key, WeightedSpanTerm value) {
      WeightedSpanTerm prev = super.put(key, value);
      if (prev == null) return prev;
      WeightedSpanTerm prevTerm = prev;
      WeightedSpanTerm newTerm = value;
      if (!prevTerm.positionSensitive) {
        newTerm.positionSensitive = false;
      }
      return prev;
    }
    
  }
  
  public boolean getExpandMultiTermQuery() {
    return expandMultiTermQuery;
  }

  public void setExpandMultiTermQuery(boolean expandMultiTermQuery) {
    this.expandMultiTermQuery = expandMultiTermQuery;
  }

  public boolean isUsePayloads() {
    return usePayloads;
  }

  public void setUsePayloads(boolean usePayloads) {
    this.usePayloads = usePayloads;
  }

  public boolean isCachedTokenStream() {
    return cachedTokenStream;
  }

  /** Returns the tokenStream which may have been wrapped in a CachingTokenFilter.
   * getWeightedSpanTerms* sets the tokenStream, so don't call this before. */
  public TokenStream getTokenStream() {
    assert tokenStream != null;
    return tokenStream;
  }
  
  /**
   * By default, {@link TokenStream}s that are not of the type
   * {@link CachingTokenFilter} are wrapped in a {@link CachingTokenFilter} to
   * ensure an efficient reset - if you are already using a different caching
   * {@link TokenStream} impl and you don't want it to be wrapped, set this to
   * false. This setting is ignored when a term vector based TokenStream is supplied,
   * since it can be reset efficiently.
   */
  public void setWrapIfNotCachingTokenFilter(boolean wrap) {
    this.wrapToCaching = wrap;
  }

  /** A threshold of number of characters to analyze. When a TokenStream based on
   * term vectors with offsets and positions are supplied, this setting
   * does not apply. */
  protected final void setMaxDocCharsToAnalyze(int maxDocCharsToAnalyze) {
    this.maxDocCharsToAnalyze = maxDocCharsToAnalyze;
  }
}
