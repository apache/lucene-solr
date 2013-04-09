package org.apache.lucene.search.highlight;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.FieldMaskingSpanQuery;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;


/**
 * Class used to extract {@link WeightedSpanTerm}s from a {@link Query} based on whether 
 * {@link Term}s from the {@link Query} are contained in a supplied {@link TokenStream}.
 */
public class WeightedSpanTermExtractor {

  private String fieldName;
  private TokenStream tokenStream;
  private String defaultField;
  private boolean expandMultiTermQuery;
  private boolean cachedTokenStream;
  private boolean wrapToCaching = true;
  private int maxDocCharsToAnalyze;
  private AtomicReader internalReader = null;


  public WeightedSpanTermExtractor() {
  }

  public WeightedSpanTermExtractor(String defaultField) {
    if (defaultField != null) {
      this.defaultField = defaultField;
    }
  }

  /**
   * Fills a <code>Map</code> with <@link WeightedSpanTerm>s using the terms from the supplied <code>Query</code>.
   * 
   * @param query
   *          Query to extract Terms from
   * @param terms
   *          Map to place created WeightedSpanTerms in
   * @throws IOException If there is a low-level I/O error
   */
  protected void extract(Query query, Map<String,WeightedSpanTerm> terms) throws IOException {
    if (query instanceof BooleanQuery) {
      BooleanClause[] queryClauses = ((BooleanQuery) query).getClauses();

      for (int i = 0; i < queryClauses.length; i++) {
        if (!queryClauses[i].isProhibited()) {
          extract(queryClauses[i].getQuery(), terms);
        }
      }
    } else if (query instanceof PhraseQuery) {
      PhraseQuery phraseQuery = ((PhraseQuery) query);
      Term[] phraseQueryTerms = phraseQuery.getTerms();
      SpanQuery[] clauses = new SpanQuery[phraseQueryTerms.length];
      for (int i = 0; i < phraseQueryTerms.length; i++) {
        clauses[i] = new SpanTermQuery(phraseQueryTerms[i]);
      }
      int slop = phraseQuery.getSlop();
      int[] positions = phraseQuery.getPositions();
      // add largest position increment to slop
      if (positions.length > 0) {
        int lastPos = positions[0];
        int largestInc = 0;
        int sz = positions.length;
        for (int i = 1; i < sz; i++) {
          int pos = positions[i];
          int inc = pos - lastPos;
          if (inc > largestInc) {
            largestInc = inc;
          }
          lastPos = pos;
        }
        if(largestInc > 1) {
          slop += largestInc;
        }
      }

      boolean inorder = false;

      if (slop == 0) {
        inorder = true;
      }

      SpanNearQuery sp = new SpanNearQuery(clauses, slop, inorder);
      sp.setBoost(query.getBoost());
      extractWeightedSpanTerms(terms, sp);
    } else if (query instanceof TermQuery) {
      extractWeightedTerms(terms, query);
    } else if (query instanceof SpanQuery) {
      extractWeightedSpanTerms(terms, (SpanQuery) query);
    } else if (query instanceof FilteredQuery) {
      extract(((FilteredQuery) query).getQuery(), terms);
    } else if (query instanceof ConstantScoreQuery) {
      final Query q = ((ConstantScoreQuery) query).getQuery();
      if (q != null) {
        extract(q, terms);
      }
    } else if (query instanceof CommonTermsQuery) {
      // specialized since rewriting would change the result query 
      // this query is TermContext sensitive.
      extractWeightedTerms(terms, query);
    } else if (query instanceof DisjunctionMaxQuery) {
      for (Iterator<Query> iterator = ((DisjunctionMaxQuery) query).iterator(); iterator.hasNext();) {
        extract(iterator.next(), terms);
      }
    } else if (query instanceof MultiPhraseQuery) {
      final MultiPhraseQuery mpq = (MultiPhraseQuery) query;
      final List<Term[]> termArrays = mpq.getTermArrays();
      final int[] positions = mpq.getPositions();
      if (positions.length > 0) {

        int maxPosition = positions[positions.length - 1];
        for (int i = 0; i < positions.length - 1; ++i) {
          if (positions[i] > maxPosition) {
            maxPosition = positions[i];
          }
        }

        @SuppressWarnings("unchecked")
        final List<SpanQuery>[] disjunctLists = new List[maxPosition + 1];
        int distinctPositions = 0;

        for (int i = 0; i < termArrays.size(); ++i) {
          final Term[] termArray = termArrays.get(i);
          List<SpanQuery> disjuncts = disjunctLists[positions[i]];
          if (disjuncts == null) {
            disjuncts = (disjunctLists[positions[i]] = new ArrayList<SpanQuery>(termArray.length));
            ++distinctPositions;
          }
          for (int j = 0; j < termArray.length; ++j) {
            disjuncts.add(new SpanTermQuery(termArray[j]));
          }
        }

        int positionGaps = 0;
        int position = 0;
        final SpanQuery[] clauses = new SpanQuery[distinctPositions];
        for (int i = 0; i < disjunctLists.length; ++i) {
          List<SpanQuery> disjuncts = disjunctLists[i];
          if (disjuncts != null) {
            clauses[position++] = new SpanOrQuery(disjuncts
                .toArray(new SpanQuery[disjuncts.size()]));
          } else {
            ++positionGaps;
          }
        }

        final int slop = mpq.getSlop();
        final boolean inorder = (slop == 0);

        SpanNearQuery sp = new SpanNearQuery(clauses, slop + positionGaps, inorder);
        sp.setBoost(query.getBoost());
        extractWeightedSpanTerms(terms, sp);
      }
    } else {
      Query origQuery = query;
      if (query instanceof MultiTermQuery) {
        if (!expandMultiTermQuery) {
          return;
        }
        MultiTermQuery copy = (MultiTermQuery) query.clone();
        copy.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
        origQuery = copy;
      }
      final IndexReader reader = getLeafContext().reader();
      Query rewritten = origQuery.rewrite(reader);
      if (rewritten != origQuery) {
        // only rewrite once and then flatten again - the rewritten query could have a speacial treatment
        // if this method is overwritten in a subclass or above in the next recursion
        extract(rewritten, terms);
      } 
    }
    extractUnknownQuery(query, terms);
  }

  protected void extractUnknownQuery(Query query,
      Map<String, WeightedSpanTerm> terms) throws IOException {
    
    // for sub-classing to extract custom queries
  }

  /**
   * Fills a <code>Map</code> with <@link WeightedSpanTerm>s using the terms from the supplied <code>SpanQuery</code>.
   * 
   * @param terms
   *          Map to place created WeightedSpanTerms in
   * @param spanQuery
   *          SpanQuery to extract Terms from
   * @throws IOException If there is a low-level I/O error
   */
  protected void extractWeightedSpanTerms(Map<String,WeightedSpanTerm> terms, SpanQuery spanQuery) throws IOException {
    Set<String> fieldNames;

    if (fieldName == null) {
      fieldNames = new HashSet<String>();
      collectSpanQueryFields(spanQuery, fieldNames);
    } else {
      fieldNames = new HashSet<String>(1);
      fieldNames.add(fieldName);
    }
    // To support the use of the default field name
    if (defaultField != null) {
      fieldNames.add(defaultField);
    }
    
    Map<String, SpanQuery> queries = new HashMap<String, SpanQuery>();
 
    Set<Term> nonWeightedTerms = new HashSet<Term>();
    final boolean mustRewriteQuery = mustRewriteQuery(spanQuery);
    if (mustRewriteQuery) {
      for (final String field : fieldNames) {
        final SpanQuery rewrittenQuery = (SpanQuery) spanQuery.rewrite(getLeafContext().reader());
        queries.put(field, rewrittenQuery);
        rewrittenQuery.extractTerms(nonWeightedTerms);
      }
    } else {
      spanQuery.extractTerms(nonWeightedTerms);
    }

    List<PositionSpan> spanPositions = new ArrayList<PositionSpan>();

    for (final String field : fieldNames) {
      final SpanQuery q;
      if (mustRewriteQuery) {
        q = queries.get(field);
      } else {
        q = spanQuery;
      }
      AtomicReaderContext context = getLeafContext();
      Map<Term,TermContext> termContexts = new HashMap<Term,TermContext>();
      TreeSet<Term> extractedTerms = new TreeSet<Term>();
      q.extractTerms(extractedTerms);
      for (Term term : extractedTerms) {
        termContexts.put(term, TermContext.build(context, term, true));
      }
      Bits acceptDocs = context.reader().getLiveDocs();
      final Spans spans = q.getSpans(context, acceptDocs, termContexts);

      // collect span positions
      while (spans.next()) {
        spanPositions.add(new PositionSpan(spans.start(), spans.end() - 1));
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
          weightedSpanTerm = new WeightedSpanTerm(spanQuery.getBoost(), queryTerm.text());
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
   * Fills a <code>Map</code> with <@link WeightedSpanTerm>s using the terms from the supplied <code>Query</code>.
   * 
   * @param terms
   *          Map to place created WeightedSpanTerms in
   * @param query
   *          Query to extract Terms from
   * @throws IOException If there is a low-level I/O error
   */
  protected void extractWeightedTerms(Map<String,WeightedSpanTerm> terms, Query query) throws IOException {
    Set<Term> nonWeightedTerms = new HashSet<Term>();
    query.extractTerms(nonWeightedTerms);

    for (final Term queryTerm : nonWeightedTerms) {

      if (fieldNameComparator(queryTerm.field())) {
        WeightedSpanTerm weightedSpanTerm = new WeightedSpanTerm(query.getBoost(), queryTerm.text());
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

  protected AtomicReaderContext getLeafContext() throws IOException {
    if (internalReader == null) {
      if(wrapToCaching && !(tokenStream instanceof CachingTokenFilter)) {
        assert !cachedTokenStream;
        tokenStream = new CachingTokenFilter(new OffsetLimitTokenFilter(tokenStream, maxDocCharsToAnalyze));
        cachedTokenStream = true;
      }
      final MemoryIndex indexer = new MemoryIndex(true);
      indexer.addField(DelegatingAtomicReader.FIELD_NAME, tokenStream);
      tokenStream.reset();
      final IndexSearcher searcher = indexer.createSearcher();
      // MEM index has only atomic ctx
      internalReader = new DelegatingAtomicReader(((AtomicReaderContext)searcher.getTopReaderContext()).reader());
    }
    return internalReader.getContext();
  }
  
  /*
   * This reader will just delegate every call to a single field in the wrapped
   * AtomicReader. This way we only need to build this field once rather than
   * N-Times
   */
  static final class DelegatingAtomicReader extends FilterAtomicReader {
    private static final String FIELD_NAME = "shadowed_field";

    DelegatingAtomicReader(AtomicReader in) {
      super(in);
    }
    
    @Override
    public FieldInfos getFieldInfos() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Fields fields() throws IOException {
      return new FilterFields(super.fields()) {
        @Override
        public Terms terms(String field) throws IOException {
          return super.terms(DelegatingAtomicReader.FIELD_NAME);
        }

        @Override
        public Iterator<String> iterator() {
          return Collections.singletonList(DelegatingAtomicReader.FIELD_NAME).iterator();
        }

        @Override
        public int size() {
          return 1;
        }
      };
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
  }

  /**
   * Creates a Map of <code>WeightedSpanTerms</code> from the given <code>Query</code> and <code>TokenStream</code>.
   * 
   * <p>
   * 
   * @param query
   *          that caused hit
   * @param tokenStream
   *          of text to be highlighted
   * @return Map containing WeightedSpanTerms
   * @throws IOException If there is a low-level I/O error
   */
  public Map<String,WeightedSpanTerm> getWeightedSpanTerms(Query query, TokenStream tokenStream)
      throws IOException {
    return getWeightedSpanTerms(query, tokenStream, null);
  }

  /**
   * Creates a Map of <code>WeightedSpanTerms</code> from the given <code>Query</code> and <code>TokenStream</code>.
   * 
   * <p>
   * 
   * @param query
   *          that caused hit
   * @param tokenStream
   *          of text to be highlighted
   * @param fieldName
   *          restricts Term's used based on field name
   * @return Map containing WeightedSpanTerms
   * @throws IOException If there is a low-level I/O error
   */
  public Map<String,WeightedSpanTerm> getWeightedSpanTerms(Query query, TokenStream tokenStream,
      String fieldName) throws IOException {
    if (fieldName != null) {
      this.fieldName = fieldName;
    } else {
      this.fieldName = null;
    }

    Map<String,WeightedSpanTerm> terms = new PositionCheckingMap<String>();
    this.tokenStream = tokenStream;
    try {
      extract(query, terms);
    } finally {
      IOUtils.close(internalReader);
    }

    return terms;
  }

  /**
   * Creates a Map of <code>WeightedSpanTerms</code> from the given <code>Query</code> and <code>TokenStream</code>. Uses a supplied
   * <code>IndexReader</code> to properly weight terms (for gradient highlighting).
   * 
   * <p>
   * 
   * @param query
   *          that caused hit
   * @param tokenStream
   *          of text to be highlighted
   * @param fieldName
   *          restricts Term's used based on field name
   * @param reader
   *          to use for scoring
   * @return Map of WeightedSpanTerms with quasi tf/idf scores
   * @throws IOException If there is a low-level I/O error
   */
  public Map<String,WeightedSpanTerm> getWeightedSpanTermsWithScores(Query query, TokenStream tokenStream, String fieldName,
      IndexReader reader) throws IOException {
    if (fieldName != null) {
      this.fieldName = fieldName;
    } else {
      this.fieldName = null;
    }
    this.tokenStream = tokenStream;

    Map<String,WeightedSpanTerm> terms = new PositionCheckingMap<String>();
    extract(query, terms);

    int totalNumDocs = reader.maxDoc();
    Set<String> weightedTerms = terms.keySet();
    Iterator<String> it = weightedTerms.iterator();

    try {
      while (it.hasNext()) {
        WeightedSpanTerm weightedSpanTerm = terms.get(it.next());
        int docFreq = reader.docFreq(new Term(fieldName, weightedSpanTerm.term));
        // IDF algorithm taken from DefaultSimilarity class
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
  
  public boolean isCachedTokenStream() {
    return cachedTokenStream;
  }
  
  public TokenStream getTokenStream() {
    return tokenStream;
  }
  
  /**
   * By default, {@link TokenStream}s that are not of the type
   * {@link CachingTokenFilter} are wrapped in a {@link CachingTokenFilter} to
   * ensure an efficient reset - if you are already using a different caching
   * {@link TokenStream} impl and you don't want it to be wrapped, set this to
   * false.
   */
  public void setWrapIfNotCachingTokenFilter(boolean wrap) {
    this.wrapToCaching = wrap;
  }

  protected final void setMaxDocCharsToAnalyze(int maxDocCharsToAnalyze) {
    this.maxDocCharsToAnalyze = maxDocCharsToAnalyze;
  }
}
