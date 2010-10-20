package org.apache.lucene.search;

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

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Comparator;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.ReaderUtil;
import org.apache.lucene.util.BytesRefHash.DirectBytesStartArray;

/**
 * An abstract {@link Query} that matches documents
 * containing a subset of terms provided by a {@link
 * FilteredTermsEnum} enumeration.
 *
 * <p>This query cannot be used directly; you must subclass
 * it and define {@link #getTermsEnum(IndexReader,AttributeSource)} to provide a {@link
 * FilteredTermsEnum} that iterates through the terms to be
 * matched.
 *
 * <p><b>NOTE</b>: if {@link #setRewriteMethod} is either
 * {@link #CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE} or {@link
 * #SCORING_BOOLEAN_QUERY_REWRITE}, you may encounter a
 * {@link BooleanQuery.TooManyClauses} exception during
 * searching, which happens when the number of terms to be
 * searched exceeds {@link
 * BooleanQuery#getMaxClauseCount()}.  Setting {@link
 * #setRewriteMethod} to {@link #CONSTANT_SCORE_FILTER_REWRITE}
 * prevents this.
 *
 * <p>The recommended rewrite method is {@link
 * #CONSTANT_SCORE_AUTO_REWRITE_DEFAULT}: it doesn't spend CPU
 * computing unhelpful scores, and it tries to pick the most
 * performant rewrite method given the query. If you
 * need scoring (like {@link FuzzyQuery}, use
 * {@link TopTermsScoringBooleanQueryRewrite} which uses
 * a priority queue to only collect competitive terms
 * and not hit this limitation.
 *
 * Note that {@link QueryParser} produces
 * MultiTermQueries using {@link
 * #CONSTANT_SCORE_AUTO_REWRITE_DEFAULT} by default.
 */
public abstract class MultiTermQuery extends Query {
  protected final String field;
  protected RewriteMethod rewriteMethod = CONSTANT_SCORE_AUTO_REWRITE_DEFAULT;
  transient int numberOfTerms = 0;
  
  /** Add this {@link Attribute} to a {@link TermsEnum} returned by {@link #getTermsEnum(IndexReader,AttributeSource)}
   * and update the boost on each returned term. This enables to control the boost factor
   * for each matching term in {@link #SCORING_BOOLEAN_QUERY_REWRITE} or
   * {@link TopTermsBooleanQueryRewrite} mode.
   * {@link FuzzyQuery} is using this to take the edit distance into account.
   * <p><b>Please note:</b> This attribute is intended to be added only by the TermsEnum
   * to itsself in its constructor and consumed by the {@link RewriteMethod}.
   * @lucene.internal
   */
  public static interface BoostAttribute extends Attribute {
    /** Sets the boost in this attribute */
    public void setBoost(float boost);
    /** Retrieves the boost, default is {@code 1.0f}. */
    public float getBoost();
  }

  /** Implementation class for {@link BoostAttribute}. */
  public static final class BoostAttributeImpl extends AttributeImpl implements BoostAttribute {
    private float boost = 1.0f;
  
    public void setBoost(float boost) {
      this.boost = boost;
    }
    
    public float getBoost() {
      return boost;
    }

    @Override
    public void clear() {
      boost = 1.0f;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other)
        return true;
      if (other instanceof BoostAttributeImpl)
        return ((BoostAttributeImpl) other).boost == boost;
      return false;
    }

    @Override
    public int hashCode() {
      return Float.floatToIntBits(boost);
    }
    
    @Override
    public void copyTo(AttributeImpl target) {
      ((BoostAttribute) target).setBoost(boost);
    }
  }

  /** Add this {@link Attribute} to a fresh {@link AttributeSource} before calling
   * {@link #getTermsEnum(IndexReader,AttributeSource)}.
   * {@link FuzzyQuery} is using this to control its internal behaviour
   * to only return competitive terms.
   * <p><b>Please note:</b> This attribute is intended to be added by the {@link RewriteMethod}
   * to an empty {@link AttributeSource} that is shared for all segments
   * during query rewrite. This attribute source is passed to all segment enums
   * on {@link #getTermsEnum(IndexReader,AttributeSource)}.
   * {@link TopTermsBooleanQueryRewrite} uses this attribute to
   * inform all enums about the current boost, that is not competitive.
   * @lucene.internal
   */
  public static interface MaxNonCompetitiveBoostAttribute extends Attribute {
    /** This is the maximum boost that would not be competitive. */
    public void setMaxNonCompetitiveBoost(float maxNonCompetitiveBoost);
    /** This is the maximum boost that would not be competitive. Default is negative infinity, which means every term is competitive. */
    public float getMaxNonCompetitiveBoost();
    /** This is the term or <code>null<code> of the term that triggered the boost change. */
    public void setCompetitiveTerm(BytesRef competitiveTerm);
    /** This is the term or <code>null<code> of the term that triggered the boost change. Default is <code>null</code>, which means every term is competitoive. */
    public BytesRef getCompetitiveTerm();
  }

  /** Implementation class for {@link MaxNonCompetitiveBoostAttribute}. */
  public static final class MaxNonCompetitiveBoostAttributeImpl extends AttributeImpl implements MaxNonCompetitiveBoostAttribute {
    private float maxNonCompetitiveBoost = Float.NEGATIVE_INFINITY;
    private BytesRef competitiveTerm = null;
  
    public void setMaxNonCompetitiveBoost(final float maxNonCompetitiveBoost) {
      this.maxNonCompetitiveBoost = maxNonCompetitiveBoost;
    }
    
    public float getMaxNonCompetitiveBoost() {
      return maxNonCompetitiveBoost;
    }

    public void setCompetitiveTerm(final BytesRef competitiveTerm) {
      this.competitiveTerm = competitiveTerm;
    }
    
    public BytesRef getCompetitiveTerm() {
      return competitiveTerm;
    }

    @Override
    public void clear() {
      maxNonCompetitiveBoost = Float.NEGATIVE_INFINITY;
      competitiveTerm = null;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other)
        return true;
      if (other instanceof MaxNonCompetitiveBoostAttributeImpl) {
        final MaxNonCompetitiveBoostAttributeImpl o = (MaxNonCompetitiveBoostAttributeImpl) other;
        return (o.maxNonCompetitiveBoost == maxNonCompetitiveBoost)
          && (o.competitiveTerm == null ? competitiveTerm == null : o.competitiveTerm.equals(competitiveTerm));
      }
      return false;
    }

    @Override
    public int hashCode() {
      int hash = Float.floatToIntBits(maxNonCompetitiveBoost);
      if (competitiveTerm != null) hash = 31 * hash + competitiveTerm.hashCode();
      return hash;
    }
    
    @Override
    public void copyTo(AttributeImpl target) {
      final MaxNonCompetitiveBoostAttributeImpl t = (MaxNonCompetitiveBoostAttributeImpl) target;
      t.setMaxNonCompetitiveBoost(maxNonCompetitiveBoost);
      t.setCompetitiveTerm(competitiveTerm);
    }
  }

  /** Abstract class that defines how the query is rewritten. */
  public static abstract class RewriteMethod implements Serializable {
    public abstract Query rewrite(IndexReader reader, MultiTermQuery query) throws IOException;
  }

  private static final class ConstantScoreFilterRewrite extends RewriteMethod {
    @Override
    public Query rewrite(IndexReader reader, MultiTermQuery query) {
      Query result = new ConstantScoreQuery(new MultiTermQueryWrapperFilter<MultiTermQuery>(query));
      result.setBoost(query.getBoost());
      return result;
    }

    // Make sure we are still a singleton even after deserializing
    protected Object readResolve() {
      return CONSTANT_SCORE_FILTER_REWRITE;
    }
  }

  /** A rewrite method that first creates a private Filter,
   *  by visiting each term in sequence and marking all docs
   *  for that term.  Matching documents are assigned a
   *  constant score equal to the query's boost.
   * 
   *  <p> This method is faster than the BooleanQuery
   *  rewrite methods when the number of matched terms or
   *  matched documents is non-trivial. Also, it will never
   *  hit an errant {@link BooleanQuery.TooManyClauses}
   *  exception.
   *
   *  @see #setRewriteMethod */
  public final static RewriteMethod CONSTANT_SCORE_FILTER_REWRITE = new ConstantScoreFilterRewrite();

  private abstract static class BooleanQueryRewrite extends RewriteMethod {
  
    protected final int collectTerms(IndexReader reader, MultiTermQuery query, TermCollector collector) throws IOException {
      final List<IndexReader> subReaders = new ArrayList<IndexReader>();
      ReaderUtil.gatherSubReaders(subReaders, reader);
      int count = 0;
      Comparator<BytesRef> lastTermComp = null;
      
      for (IndexReader r : subReaders) {
        final Fields fields = r.fields();
        if (fields == null) {
          // reader has no fields
          continue;
        }

        final Terms terms = fields.terms(query.field);
        if (terms == null) {
          // field does not exist
          continue;
        }

        final TermsEnum termsEnum = query.getTermsEnum(r, collector.attributes);
        assert termsEnum != null;

        if (termsEnum == TermsEnum.EMPTY)
          continue;
        
        // Check comparator compatibility:
        final Comparator<BytesRef> newTermComp = termsEnum.getComparator();
        if (lastTermComp != null && newTermComp != lastTermComp)
          throw new RuntimeException("term comparator should not change between segments: "+lastTermComp+" != "+newTermComp);
        lastTermComp = newTermComp;
        
        collector.setNextEnum(termsEnum);
        BytesRef bytes;
        while ((bytes = termsEnum.next()) != null) {
          if (collector.collect(bytes)) {
            termsEnum.cacheCurrentTerm();
            count++;
          } else {
            return count; // interrupt whole term collection, so also don't iterate other subReaders
          }
        }
      }
      return count;
    }
    
    protected static abstract class TermCollector {
      /** attributes used for communication with the enum */
      public final AttributeSource attributes = new AttributeSource();
    
      /** return false to stop collecting */
      public abstract boolean collect(BytesRef bytes) throws IOException;
      
      /** the next segment's {@link TermsEnum} that is used to collect terms */
      public abstract void setNextEnum(TermsEnum termsEnum) throws IOException;
    }
  }
  
  private static class ScoringBooleanQueryRewrite extends BooleanQueryRewrite {
    @Override
    public Query rewrite(final IndexReader reader, final MultiTermQuery query) throws IOException {
      final ParallelArraysTermCollector col = new ParallelArraysTermCollector();
      collectTerms(reader, query, col);
      
      final Term placeholderTerm = new Term(query.field);
      final BooleanQuery result = new BooleanQuery(true);
      final int size = col.terms.size();
      if (size > 0) {
        final int sort[] = col.terms.sort(col.termsEnum.getComparator());
        final int[] docFreq = col.array.docFreq;
        final float[] boost = col.array.boost;
        for (int i = 0; i < size; i++) {
          final int pos = sort[i];
          final Term term = placeholderTerm.createTerm(col.terms.get(pos, new BytesRef()));
          assert reader.docFreq(term) == docFreq[pos];
          final TermQuery tq = new TermQuery(term, docFreq[pos]);
          tq.setBoost(query.getBoost() * boost[pos]);
          result.add(tq, BooleanClause.Occur.SHOULD);
        }
      }
      query.incTotalNumberOfTerms(size);
      return result;
    }

    // Make sure we are still a singleton even after deserializing
    protected Object readResolve() {
      return SCORING_BOOLEAN_QUERY_REWRITE;
    }
    
    static final class ParallelArraysTermCollector extends TermCollector {
      final TermFreqBoostByteStart array = new TermFreqBoostByteStart(16);
      final BytesRefHash terms = new BytesRefHash(new ByteBlockPool(new ByteBlockPool.DirectAllocator()), 16, array);
      TermsEnum termsEnum;

      private BoostAttribute boostAtt;
    
      @Override
      public void setNextEnum(TermsEnum termsEnum) throws IOException {
        this.termsEnum = termsEnum;
        this.boostAtt = termsEnum.attributes().addAttribute(BoostAttribute.class);
      }
    
      @Override
      public boolean collect(BytesRef bytes) {
        final int e = terms.add(bytes);
        if (e < 0 ) {
          // duplicate term: update docFreq
          final int pos = (-e)-1;
          array.docFreq[pos] += termsEnum.docFreq();
          assert array.boost[pos] == boostAtt.getBoost() : "boost should be equal in all segment TermsEnums";
        } else {
          // new entry: we populate the entry initially
          array.docFreq[e] = termsEnum.docFreq();
          array.boost[e] = boostAtt.getBoost();
        }
        // if the new entry reaches the max clause count, we exit early
        if (e >= BooleanQuery.getMaxClauseCount())
          throw new BooleanQuery.TooManyClauses();
        return true;
      }
    }
    
    /** Special implementation of BytesStartArray that keeps parallel arrays for boost and docFreq */
    static final class TermFreqBoostByteStart extends DirectBytesStartArray  {
      int[] docFreq;
      float[] boost;
      
      public TermFreqBoostByteStart(int initSize) {
        super(initSize);
      }

      @Override
      public int[] init() {
        final int[] ord = super.init();
        boost = new float[ArrayUtil.oversize(ord.length, RamUsageEstimator.NUM_BYTES_FLOAT)];
        docFreq = new int[ArrayUtil.oversize(ord.length, RamUsageEstimator.NUM_BYTES_INT)];
        assert boost.length >= ord.length && docFreq.length >= ord.length;
        return ord;
      }

      @Override
      public int[] grow() {
        final int[] ord = super.grow();
        docFreq = ArrayUtil.grow(docFreq, ord.length);
        boost = ArrayUtil.grow(boost, ord.length);
        assert boost.length >= ord.length && docFreq.length >= ord.length;
        return ord;
      }

      @Override
      public int[] clear() {
       boost = null;
       docFreq = null;
       return super.clear();
      }
      
    }
  }

  /** A rewrite method that first translates each term into
   *  {@link BooleanClause.Occur#SHOULD} clause in a
   *  BooleanQuery, and keeps the scores as computed by the
   *  query.  Note that typically such scores are
   *  meaningless to the user, and require non-trivial CPU
   *  to compute, so it's almost always better to use {@link
   *  #CONSTANT_SCORE_AUTO_REWRITE_DEFAULT} instead.
   *
   *  <p><b>NOTE</b>: This rewrite method will hit {@link
   *  BooleanQuery.TooManyClauses} if the number of terms
   *  exceeds {@link BooleanQuery#getMaxClauseCount}.
   *
   *  @see #setRewriteMethod */
  public final static RewriteMethod SCORING_BOOLEAN_QUERY_REWRITE = new ScoringBooleanQueryRewrite();


  /**
   * Base rewrite method for collecting only the top terms
   * via a priority queue.
   */
  public static abstract class TopTermsBooleanQueryRewrite extends BooleanQueryRewrite {
    private final int size;
    
    /** 
     * Create a TopTermsBooleanQueryRewrite for 
     * at most <code>size</code> terms.
     * <p>
     * NOTE: if {@link BooleanQuery#getMaxClauseCount} is smaller than 
     * <code>size</code>, then it will be used instead. 
     */
    public TopTermsBooleanQueryRewrite(int size) {
      this.size = size;
    }
    
    /** Return a suitable Query for a MultiTermQuery term. */
    protected abstract Query getQuery(Term term, int docCount);

    @Override
    public Query rewrite(final IndexReader reader, final MultiTermQuery query) throws IOException {
      final int maxSize = Math.min(size, BooleanQuery.getMaxClauseCount());
      final PriorityQueue<ScoreTerm> stQueue = new PriorityQueue<ScoreTerm>();
      collectTerms(reader, query, new TermCollector() {
        private final MaxNonCompetitiveBoostAttribute maxBoostAtt =
          attributes.addAttribute(MaxNonCompetitiveBoostAttribute.class);
        
        private final Map<BytesRef,ScoreTerm> visitedTerms = new HashMap<BytesRef,ScoreTerm>();
        
        private TermsEnum termsEnum;
        private Comparator<BytesRef> termComp;
        private BoostAttribute boostAtt;        
        private ScoreTerm st;
        
        @Override
        public void setNextEnum(TermsEnum termsEnum) throws IOException {
          this.termsEnum = termsEnum;
          this.termComp = termsEnum.getComparator();
          // lazy init the initial ScoreTerm because comparator is not known on ctor:
          if (st == null)
            st = new ScoreTerm(this.termComp);
          boostAtt = termsEnum.attributes().addAttribute(BoostAttribute.class);
        }
      
        @Override
        public boolean collect(BytesRef bytes) {
          final float boost = boostAtt.getBoost();
          // ignore uncompetetive hits
          if (stQueue.size() == maxSize) {
            final ScoreTerm t = stQueue.peek();
            if (boost < t.boost)
              return true;
            if (boost == t.boost && termComp.compare(bytes, t.bytes) > 0)
              return true;
          }
          ScoreTerm t = visitedTerms.get(bytes);
          if (t != null) {
            // if the term is already in the PQ, only update docFreq of term in PQ
            t.docFreq += termsEnum.docFreq();
            assert t.boost == boost : "boost should be equal in all segment TermsEnums";
          } else {
            // add new entry in PQ, we must clone the term, else it may get overwritten!
            st.bytes.copy(bytes);
            st.boost = boost;
            st.docFreq = termsEnum.docFreq();
            visitedTerms.put(st.bytes, st);
            stQueue.offer(st);
            // possibly drop entries from queue
            if (stQueue.size() > maxSize) {
              st = stQueue.poll();
              visitedTerms.remove(st.bytes);
            } else {
              st = new ScoreTerm(termComp);
            }
            assert stQueue.size() <= maxSize : "the PQ size must be limited to maxSize";
            // set maxBoostAtt with values to help FuzzyTermsEnum to optimize
            if (stQueue.size() == maxSize) {
              t = stQueue.peek();
              maxBoostAtt.setMaxNonCompetitiveBoost(t.boost);
              maxBoostAtt.setCompetitiveTerm(t.bytes);
            }
          }
          return true;
        }
      });
      
      final Term placeholderTerm = new Term(query.field);
      final BooleanQuery bq = new BooleanQuery(true);
      final ScoreTerm[] scoreTerms = stQueue.toArray(new ScoreTerm[stQueue.size()]);
      Arrays.sort(scoreTerms, new Comparator<ScoreTerm>() {
        public int compare(ScoreTerm st1, ScoreTerm st2) {
          assert st1.termComp == st2.termComp :
            "term comparator should not change between segments";
          return st1.termComp.compare(st1.bytes, st2.bytes);
        }
      });
      for (final ScoreTerm st : scoreTerms) {
        final Term term = placeholderTerm.createTerm(st.bytes);
        assert reader.docFreq(term) == st.docFreq;
        Query tq = getQuery(term, st.docFreq);
        tq.setBoost(query.getBoost() * st.boost); // set the boost
        bq.add(tq, BooleanClause.Occur.SHOULD);   // add to query
      }
      query.incTotalNumberOfTerms(scoreTerms.length);
      return bq;
    }
  
    @Override
    public int hashCode() {
      return 31 * size;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      TopTermsBooleanQueryRewrite other = (TopTermsBooleanQueryRewrite) obj;
      if (size != other.size) return false;
      return true;
    }
  
    static final class ScoreTerm implements Comparable<ScoreTerm> {
      public final Comparator<BytesRef> termComp;

      public final BytesRef bytes = new BytesRef();
      public float boost;
      public int docFreq;
      
      public ScoreTerm(Comparator<BytesRef> termComp) {
        this.termComp = termComp;
      }
      
      public int compareTo(ScoreTerm other) {
        if (this.boost == other.boost)
          return termComp.compare(other.bytes, this.bytes);
        else
          return Float.compare(this.boost, other.boost);
      }
    }
  }

  /**
   * A rewrite method that first translates each term into
   * {@link BooleanClause.Occur#SHOULD} clause in a BooleanQuery, and keeps the
   * scores as computed by the query.
   * 
   * <p>
   * This rewrite method only uses the top scoring terms so it will not overflow
   * the boolean max clause count. It is the default rewrite method for
   * {@link FuzzyQuery}.
   * 
   * @see #setRewriteMethod
   */
  public static final class TopTermsScoringBooleanQueryRewrite extends
      TopTermsBooleanQueryRewrite {

    /** 
     * Create a TopTermsScoringBooleanQueryRewrite for 
     * at most <code>size</code> terms.
     * <p>
     * NOTE: if {@link BooleanQuery#getMaxClauseCount} is smaller than 
     * <code>size</code>, then it will be used instead. 
     */
    public TopTermsScoringBooleanQueryRewrite(int size) {
      super(size);
    }
    
    @Override
    protected Query getQuery(Term term, int docFreq) {
      return new TermQuery(term, docFreq);
    }
  }
  
  /**
   * A rewrite method that first translates each term into
   * {@link BooleanClause.Occur#SHOULD} clause in a BooleanQuery, but the scores
   * are only computed as the boost.
   * <p>
   * This rewrite method only uses the top scoring terms so it will not overflow
   * the boolean max clause count.
   * 
   * @see #setRewriteMethod
   */
  public static final class TopTermsBoostOnlyBooleanQueryRewrite extends
      TopTermsBooleanQueryRewrite {
    
    /** 
     * Create a TopTermsBoostOnlyBooleanQueryRewrite for 
     * at most <code>size</code> terms.
     * <p>
     * NOTE: if {@link BooleanQuery#getMaxClauseCount} is smaller than 
     * <code>size</code>, then it will be used instead. 
     */
    public TopTermsBoostOnlyBooleanQueryRewrite(int size) {
      super(size);
    }
    
    @Override
    protected Query getQuery(Term term, int docFreq) {
      return new ConstantScoreQuery(new QueryWrapperFilter(new TermQuery(term, docFreq)));
    }
  }
  
  private static class ConstantScoreBooleanQueryRewrite extends ScoringBooleanQueryRewrite implements Serializable {
    @Override
    public Query rewrite(IndexReader reader, MultiTermQuery query) throws IOException {
      Query result = super.rewrite(reader, query);
      assert result instanceof BooleanQuery;
      // TODO: if empty boolean query return NullQuery?
      if (!((BooleanQuery) result).clauses().isEmpty()) {
        // strip the scores off
        result = new ConstantScoreQuery(new QueryWrapperFilter(result));
        result.setBoost(query.getBoost());
      }
      return result;
    }

    // Make sure we are still a singleton even after deserializing
    @Override
    protected Object readResolve() {
      return CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE;
    }
  }

  /** Like {@link #SCORING_BOOLEAN_QUERY_REWRITE} except
   *  scores are not computed.  Instead, each matching
   *  document receives a constant score equal to the
   *  query's boost.
   * 
   *  <p><b>NOTE</b>: This rewrite method will hit {@link
   *  BooleanQuery.TooManyClauses} if the number of terms
   *  exceeds {@link BooleanQuery#getMaxClauseCount}.
   *
   *  @see #setRewriteMethod */
  public final static RewriteMethod CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE = new ConstantScoreBooleanQueryRewrite();


  /** A rewrite method that tries to pick the best
   *  constant-score rewrite method based on term and
   *  document counts from the query.  If both the number of
   *  terms and documents is small enough, then {@link
   *  #CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE} is used.
   *  Otherwise, {@link #CONSTANT_SCORE_FILTER_REWRITE} is
   *  used.
   */
  public static class ConstantScoreAutoRewrite extends BooleanQueryRewrite {

    // Defaults derived from rough tests with a 20.0 million
    // doc Wikipedia index.  With more than 350 terms in the
    // query, the filter method is fastest:
    public static int DEFAULT_TERM_COUNT_CUTOFF = 350;

    // If the query will hit more than 1 in 1000 of the docs
    // in the index (0.1%), the filter method is fastest:
    public static double DEFAULT_DOC_COUNT_PERCENT = 0.1;

    private int termCountCutoff = DEFAULT_TERM_COUNT_CUTOFF;
    private double docCountPercent = DEFAULT_DOC_COUNT_PERCENT;

    /** If the number of terms in this query is equal to or
     *  larger than this setting then {@link
     *  #CONSTANT_SCORE_FILTER_REWRITE} is used. */
    public void setTermCountCutoff(int count) {
      termCountCutoff = count;
    }

    /** @see #setTermCountCutoff */
    public int getTermCountCutoff() {
      return termCountCutoff;
    }

    /** If the number of documents to be visited in the
     *  postings exceeds this specified percentage of the
     *  maxDoc() for the index, then {@link
     *  #CONSTANT_SCORE_FILTER_REWRITE} is used.
     *  @param percent 0.0 to 100.0 */
    public void setDocCountPercent(double percent) {
      docCountPercent = percent;
    }

    /** @see #setDocCountPercent */
    public double getDocCountPercent() {
      return docCountPercent;
    }

    @Override
    public Query rewrite(final IndexReader reader, final MultiTermQuery query) throws IOException {

      // Get the enum and start visiting terms.  If we
      // exhaust the enum before hitting either of the
      // cutoffs, we use ConstantBooleanQueryRewrite; else,
      // ConstantFilterRewrite:
      final int docCountCutoff = (int) ((docCountPercent / 100.) * reader.maxDoc());
      final int termCountLimit = Math.min(BooleanQuery.getMaxClauseCount(), termCountCutoff);

      final CutOffTermCollector col = new CutOffTermCollector(docCountCutoff, termCountLimit);
      collectTerms(reader, query, col);
      final int size = col.pendingTerms.size();
      if (col.hasCutOff) {
        return CONSTANT_SCORE_FILTER_REWRITE.rewrite(reader, query);
      } else if (size == 0) {
        return new BooleanQuery(true);
      } else {
        final BooleanQuery bq = new BooleanQuery(true);
        final Term placeholderTerm = new Term(query.field);
        final BytesRefHash pendingTerms = col.pendingTerms;
        final int sort[] = pendingTerms.sort(col.termsEnum.getComparator());
        for(int i = 0; i < size; i++) {
          // docFreq is not used for constant score here, we pass 1
          // to explicitely set a fake value, so it's not calculated
          bq.add(new TermQuery(
            placeholderTerm.createTerm(pendingTerms.get(sort[i], new BytesRef())), 1
          ), BooleanClause.Occur.SHOULD);
        }
        // Strip scores
        final Query result = new ConstantScoreQuery(new QueryWrapperFilter(bq));
        result.setBoost(query.getBoost());
        query.incTotalNumberOfTerms(size);
        return result;
      }
    }
    
    static final class CutOffTermCollector extends TermCollector {
      CutOffTermCollector(int docCountCutoff, int termCountLimit) {
        this.docCountCutoff = docCountCutoff;
        this.termCountLimit = termCountLimit;
      }
    
      @Override
      public void setNextEnum(TermsEnum termsEnum) throws IOException {
        this.termsEnum = termsEnum;
      }
        
      @Override
      public boolean collect(BytesRef bytes) throws IOException {
        if (pendingTerms.size() >= termCountLimit || docVisitCount >= docCountCutoff) {
          hasCutOff = true;
          return false;
        }
        pendingTerms.add(bytes);
        docVisitCount += termsEnum.docFreq();
        return true;
      }
      
      int docVisitCount = 0;
      boolean hasCutOff = false;
      TermsEnum termsEnum;

      final int docCountCutoff, termCountLimit;
      final BytesRefHash pendingTerms = new BytesRefHash();
    }

    @Override
    public int hashCode() {
      final int prime = 1279;
      return (int) (prime * termCountCutoff + Double.doubleToLongBits(docCountPercent));
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;

      ConstantScoreAutoRewrite other = (ConstantScoreAutoRewrite) obj;
      if (other.termCountCutoff != termCountCutoff) {
        return false;
      }

      if (Double.doubleToLongBits(other.docCountPercent) != Double.doubleToLongBits(docCountPercent)) {
        return false;
      }
      
      return true;
    }
  }

  /** Read-only default instance of {@link
   *  ConstantScoreAutoRewrite}, with {@link
   *  ConstantScoreAutoRewrite#setTermCountCutoff} set to
   *  {@link
   *  ConstantScoreAutoRewrite#DEFAULT_TERM_COUNT_CUTOFF}
   *  and {@link
   *  ConstantScoreAutoRewrite#setDocCountPercent} set to
   *  {@link
   *  ConstantScoreAutoRewrite#DEFAULT_DOC_COUNT_PERCENT}.
   *  Note that you cannot alter the configuration of this
   *  instance; you'll need to create a private instance
   *  instead. */
  public final static RewriteMethod CONSTANT_SCORE_AUTO_REWRITE_DEFAULT = new ConstantScoreAutoRewrite() {
    @Override
    public void setTermCountCutoff(int count) {
      throw new UnsupportedOperationException("Please create a private instance");
    }

    @Override
    public void setDocCountPercent(double percent) {
      throw new UnsupportedOperationException("Please create a private instance");
    }

    // Make sure we are still a singleton even after deserializing
    protected Object readResolve() {
      return CONSTANT_SCORE_AUTO_REWRITE_DEFAULT;
    }
  };

  /**
   * Constructs a query matching terms that cannot be represented with a single
   * Term.
   */
  public MultiTermQuery(final String field) {
    this.field = field;
    assert field != null;
  }

  /** Returns the field name for this query */
  public final String getField() { return field; }

  /** Construct the enumeration to be used, expanding the
   *  pattern term.  This method should only be called if
   *  the field exists (ie, implementations can assume the
   *  field does exist).  This method should not return null
   *  (should instead return {@link TermsEnum#EMPTY} if no
   *  terms match).  The TermsEnum must already be
   *  positioned to the first matching term.
   * The given {@link AttributeSource} is passed by the {@link RewriteMethod} to
   * provide attributes, the rewrite method uses to inform about e.g. maximum competitive boosts.
   * This is currently only used by {@link TopTermsBooleanQueryRewrite}
   */
  protected abstract TermsEnum getTermsEnum(IndexReader reader, AttributeSource atts) throws IOException;

  /** Convenience method, if no attributes are needed:
   * This simply passes empty attributes and is equal to:
   * <code>getTermsEnum(reader, new AttributeSource())</code>
   */
  protected final TermsEnum getTermsEnum(IndexReader reader) throws IOException {
    return getTermsEnum(reader, new AttributeSource());
  }

  /**
   * Expert: Return the number of unique terms visited during execution of the query.
   * If there are many of them, you may consider using another query type
   * or optimize your total term count in index.
   * <p>This method is not thread safe, be sure to only call it when no query is running!
   * If you re-use the same query instance for another
   * search, be sure to first reset the term counter
   * with {@link #clearTotalNumberOfTerms}.
   * <p>On optimized indexes / no MultiReaders, you get the correct number of
   * unique terms for the whole index. Use this number to compare different queries.
   * For non-optimized indexes this number can also be achieved in
   * non-constant-score mode. In constant-score mode you get the total number of
   * terms seeked for all segments / sub-readers.
   * @see #clearTotalNumberOfTerms
   */
  public int getTotalNumberOfTerms() {
    return numberOfTerms;
  }
  
  /**
   * Expert: Resets the counting of unique terms.
   * Do this before executing the query/filter.
   * @see #getTotalNumberOfTerms
   */
  public void clearTotalNumberOfTerms() {
    numberOfTerms = 0;
  }
  
  protected void incTotalNumberOfTerms(int inc) {
    numberOfTerms += inc;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    return rewriteMethod.rewrite(reader, this);
  }

  /**
   * @see #setRewriteMethod
   */
  public RewriteMethod getRewriteMethod() {
    return rewriteMethod;
  }

  /**
   * Sets the rewrite method to be used when executing the
   * query.  You can use one of the four core methods, or
   * implement your own subclass of {@link RewriteMethod}. */
  public void setRewriteMethod(RewriteMethod method) {
    rewriteMethod = method;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Float.floatToIntBits(getBoost());
    result = prime * result + rewriteMethod.hashCode();
    if (field != null) result = prime * result + field.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MultiTermQuery other = (MultiTermQuery) obj;
    if (Float.floatToIntBits(getBoost()) != Float.floatToIntBits(other.getBoost()))
      return false;
    if (!rewriteMethod.equals(other.rewriteMethod)) {
      return false;
    }
    return (other.field == null ? field == null : other.field.equals(field));
  }
 
}
