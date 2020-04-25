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
package org.apache.solr.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocSetBuilder;
import org.apache.solr.search.DocSetProducer;
import org.apache.solr.search.DocSetUtil;
import org.apache.solr.search.ExtendedQueryBase;
import org.apache.solr.search.Filter;
import org.apache.solr.search.SolrIndexSearcher;

/** @lucene.experimental */
public final class SolrRangeQuery extends ExtendedQueryBase implements DocSetProducer {
  private final String field;
  private final BytesRef lower;
  private final BytesRef upper;
  private byte flags;
  private static byte FLAG_INC_LOWER = 0x01;
  private static byte FLAG_INC_UPPER = 0x02;

  public SolrRangeQuery(String field, BytesRef lower, BytesRef upper, boolean includeLower, boolean includeUpper) {
    this.field = field;
    this.lower = lower;
    this.upper = upper;
    this.flags = (byte)((this.lower != null && includeLower ? FLAG_INC_LOWER : 0) | (this.upper != null && includeUpper ? FLAG_INC_UPPER : 0));
  }

  public String getField() {
    return field;
  }

  public BytesRef getLower() { return lower; }
  public BytesRef getUpper() { return upper; }

  public boolean includeLower() {
    return (flags & FLAG_INC_LOWER) != 0;
  }

  public boolean includeUpper() {
    return (flags & FLAG_INC_UPPER) != 0;
  }

  @Override
  public int hashCode() {
    int hash = 0x8f2c9ba7 * (flags+1);  // avoid multiplying by 0
    hash = hash * 29 + ((lower == null) ? 0 : lower.hashCode());  // TODO: simpler hash code here?
    hash = hash * 29 + ((upper == null) ? 0 : upper.hashCode());
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof SolrRangeQuery)) {
      return false;
    }
    SolrRangeQuery other = (SolrRangeQuery)obj;

    return (this.flags == other.flags)
        && (this.field.equals(other.field))
        && (this.lower == other.lower || (this.lower != null && other.lower != null && this.lower.equals(other.lower)))
        && (this.upper == other.upper || (this.upper != null && other.upper != null && this.upper.equals(other.upper)))
        ;
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    if (!getField().equals(field)) {
      buffer.append(getField());
      buffer.append(":");
    }
    // TODO: use our schema?
    buffer.append(includeLower() ? '[' : '{');
    buffer.append(endpoint(lower));
    buffer.append(" TO ");
    buffer.append(endpoint(upper));
    buffer.append(includeUpper() ? ']' : '}');
    return buffer.toString();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  private String endpoint(BytesRef ref) {
    if (ref == null) return "*";
    String toStr = Term.toString(ref);
    if ("*".equals(toStr)) {
      toStr = "\\*";
    }
    // TODO: other escaping
    return toStr;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    return this;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    return new ConstWeight(searcher, scoreMode, boost);
    /*
    DocSet docs = createDocSet(searcher.getIndexReader().leaves(), searcher.getIndexReader().maxDoc());
    SolrConstantScoreQuery csq = new SolrConstantScoreQuery( docs.getTopFilter() );
    return csq.createWeight(searcher, needScores);
    */
  }

  @Override
  public DocSet createDocSet(SolrIndexSearcher searcher) throws IOException {
    return createDocSet( searcher, Math.min(64,(searcher.maxDoc()>>>10)+4) );
  }

  private DocSet createDocSet(SolrIndexSearcher searcher, long cost) throws IOException {
    int maxDoc = searcher.maxDoc();
    BitDocSet liveDocs = searcher.getLiveDocSet();
    FixedBitSet liveBits = liveDocs.size() == maxDoc ? null : liveDocs.getBits();

    DocSetBuilder builder = new DocSetBuilder(maxDoc, cost);

    List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();

    int maxTermsPerSegment = 0;
    for (LeafReaderContext ctx : leaves) {
      TermsEnum te = getTermsEnum(ctx);
      int termsVisited = builder.add(te, ctx.docBase);
      maxTermsPerSegment = Math.max(maxTermsPerSegment, termsVisited);
    }

    DocSet set =  maxTermsPerSegment <= 1 ? builder.buildUniqueInOrder(liveBits) : builder.build(liveBits);
    return DocSetUtil.getDocSet(set, searcher);
  }


  private class RangeTermsEnum extends BaseTermsEnum {

    TermsEnum te;
    BytesRef curr;
    boolean positioned;

    public RangeTermsEnum(Terms terms) throws IOException {
      if (terms == null) {
        positioned = true;
      } else {
        te = terms.iterator();
        if (lower != null) {
          TermsEnum.SeekStatus status = te.seekCeil(lower);
          if (status == TermsEnum.SeekStatus.END) {
            positioned = true;
            curr = null;
          } else if (status == SeekStatus.FOUND) {
            positioned = includeLower();
            curr = te.term();
          } else {
            // lower bound not found, so includeLower is irrelevant
            positioned = true;
            curr = te.term();
          }
        }
      }
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
      return te.seekCeil(text);
    }

    @Override
    public void seekExact(long ord) throws IOException {
      te.seekExact(ord);
    }

    @Override
    public BytesRef term() throws IOException {
      return te.term(); // should be equal to curr, except if we went past the end
    }

    @Override
    public long ord() throws IOException {
      return te.ord();
    }

    @Override
    public int docFreq() throws IOException {
      return te.docFreq();
    }

    @Override
    public long totalTermFreq() throws IOException {
      return te.totalTermFreq();
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      return te.postings(reuse, flags);
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
      return te.impacts(flags);
    }

    @Override
    public BytesRef next() throws IOException {
      if (positioned) {
        positioned = false;
      } else {
        curr = te.next();
      }

      if (curr == null) return null;

      if (upper != null) {
        int cmp = curr.compareTo(upper);
        if (cmp < 0 || cmp == 0 && includeUpper()) {
          return curr;
        } else {
          curr = null;
        }
      }
      return curr;
    }

    @Override
    public AttributeSource attributes() {
      return te.attributes();
    }

    @Override
    public boolean seekExact(BytesRef text) throws IOException {
      return te.seekExact(text);
    }

    @Override
    public void seekExact(BytesRef term, TermState state) throws IOException {
      te.seekExact(term, state);
    }

    @Override
    public TermState termState() throws IOException {
      return te.termState();
    }
  }


  public TermsEnum getTermsEnum(LeafReaderContext ctx) throws IOException {
    return new RangeTermsEnum( ctx.reader().terms(getField()) );
  }


  private static class TermAndState {
    final BytesRef term;
    final TermState state;
    final int docFreq;
    final long totalTermFreq;

    TermAndState(BytesRef term, TermState state, int docFreq, long totalTermFreq) {
      this.term = term;
      this.state = state;
      this.docFreq = docFreq;
      this.totalTermFreq = totalTermFreq;
    }
  }

  private static class SegState {
    final Weight weight;
    final DocIdSet set;

    SegState(Weight weight) {
      this.weight = weight;
      this.set = null;
    }

    SegState(DocIdSet set) {
      this.set = set;
      this.weight = null;
    }
  }

  // adapted from MultiTermQueryConstantScoreWrapper
  class ConstWeight extends ConstantScoreWeight {

    private static final int BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD = 16;

    final IndexSearcher searcher;
    final ScoreMode scoreMode;
    boolean checkedFilterCache;
    Filter filter;
    final SegState[] segStates;


    protected ConstWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
      super( SolrRangeQuery.this, boost );
      this.searcher = searcher;
      this.segStates = new SegState[ searcher.getIndexReader().leaves().size() ];
      this.scoreMode = scoreMode;
    }

    // See MultiTermQueryConstantScoreWrapper matches()
    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      SolrRangeQuery query = SolrRangeQuery.this;
      final Terms terms = context.reader().terms(query.field);
      if (terms == null) {
        return null;
      }
      if (terms.hasPositions() == false) {
        return super.matches(context, doc);
      }
      return MatchesUtils.forField(query.field, () -> MatchesUtils.disjunction(context, doc, query, query.field, query.getTermsEnum(context)));
    }

    /** Try to collect terms from the given terms enum and return count=sum(df) for terms visited so far
     *  or (-count - 1) if this should be rewritten into a boolean query.
     *  The termEnum will already be positioned on the next term if not exhausted.
     */
    private long collectTerms(LeafReaderContext context, TermsEnum termsEnum, List<TermAndState> terms) throws IOException {
      long count = 0;
      final int threshold = Math.min(BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD, IndexSearcher.getMaxClauseCount());
      for (int i = 0; i < threshold; ++i) {
        final BytesRef term = termsEnum.next();
        if (term == null) {
          return -count - 1;
        }
        TermState state = termsEnum.termState();
        int df = termsEnum.docFreq();
        count += df;
        terms.add(new TermAndState(BytesRef.deepCopyOf(term), state, df, termsEnum.totalTermFreq()));
      }
      return termsEnum.next() == null ? (-count - 1) : count;
    }

    private SegState getSegState(LeafReaderContext context) throws IOException {
      SegState segState = segStates[context.ord];
      if (segState != null) return segState;

      // first time, check our filter cache
      boolean doCheck = !checkedFilterCache && context.ord == 0;
      checkedFilterCache = true;
      SolrIndexSearcher solrSearcher = null;
      if (doCheck && searcher instanceof SolrIndexSearcher) {
        solrSearcher = (SolrIndexSearcher)searcher;
        if (solrSearcher.getFilterCache() == null) {
          doCheck = false;
        } else {
          solrSearcher = (SolrIndexSearcher)searcher;
          DocSet answer = solrSearcher.getFilterCache().get(SolrRangeQuery.this);
          if (answer != null) {
            filter = answer.getTopFilter();
          }
        }
      } else {
        doCheck = false;
      }
      
      if (filter != null) {
        return segStates[context.ord] = new SegState(filter.getDocIdSet(context, null));
      }

      final Terms terms = context.reader().terms(SolrRangeQuery.this.getField());
      if (terms == null) {
        return segStates[context.ord] = new SegState((DocIdSet) null);
      }

      final TermsEnum termsEnum = SolrRangeQuery.this.getTermsEnum(context);

      PostingsEnum docs = null;

      final List<TermAndState> collectedTerms = new ArrayList<>();
      long count = collectTerms(context, termsEnum, collectedTerms);
      if (count < 0) {
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        for (TermAndState t : collectedTerms) {
          final TermStates termStates = new TermStates(searcher.getTopReaderContext());
          termStates.register(t.state, context.ord, t.docFreq, t.totalTermFreq);
          bq.add(new TermQuery(new Term( SolrRangeQuery.this.getField(), t.term), termStates), BooleanClause.Occur.SHOULD);
        }
        Query q = new ConstantScoreQuery(bq.build());
        final Weight weight = searcher.rewrite(q).createWeight(searcher, scoreMode, score());
        return segStates[context.ord] = new SegState(weight);
      }

      // Too many terms for boolean query...

      if (doCheck) {
        DocSet answer = createDocSet(solrSearcher, count);
        solrSearcher.getFilterCache().put(SolrRangeQuery.this, answer);
        filter = answer.getTopFilter();
        return segStates[context.ord] = new SegState(filter.getDocIdSet(context, null));
      }

      /* FUTURE: reuse term states in the future to help build DocSet, use collected count so far...
      Bits liveDocs = context.reader().getLiveDocs();
      int base = context.docBase;
      int termsVisited = collectedTerms.size();

      DocSetBuilder builder = new DocSetBuilder(searcher.getIndexReader().maxDoc());
      if (!collectedTerms.isEmpty()) {
        TermsEnum termsEnum2 = terms.iterator();
        for (TermAndState t : collectedTerms) {
          termsEnum2.seekExact(t.term, t.state);
          docs = termsEnum2.postings(docs, PostingsEnum.NONE);
          builder.add(docs, context.docBase, liveDocs);
        }
      }

      termsVisited += builder.add(termsEnum, base, liveDocs);
     */

      DocIdSetBuilder builder = new DocIdSetBuilder(context.reader().maxDoc(), terms);
      builder.grow((int)Math.min(Integer.MAX_VALUE,count));
      if (collectedTerms.isEmpty() == false) {
        TermsEnum termsEnum2 = terms.iterator();
        for (TermAndState t : collectedTerms) {
          termsEnum2.seekExact(t.term, t.state);
          docs = termsEnum2.postings(docs, PostingsEnum.NONE);
          builder.add(docs);
        }
      }

      do {
        // already positioned on the next term, so don't call next() here...
        docs = termsEnum.postings(docs, PostingsEnum.NONE);
        builder.add(docs);
      } while (termsEnum.next() != null);

      DocIdSet segSet = builder.build();
      return segStates[context.ord] = new SegState(segSet);
    }

    private Scorer scorer(DocIdSet set) throws IOException {
      if (set == null) {
        return null;
      }
      final DocIdSetIterator disi = set.iterator();
      if (disi == null) {
        return null;
      }
      return new ConstantScoreScorer(this, score(), scoreMode, disi);
    }

    @Override
    public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
      final SegState weightOrBitSet = getSegState(context);
      if (weightOrBitSet.weight != null) {
        return weightOrBitSet.weight.bulkScorer(context);
      } else {
        final Scorer scorer = scorer(weightOrBitSet.set);
        if (scorer == null) {
          return null;
        }
        return new DefaultBulkScorer(scorer);
      }
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      final SegState weightOrBitSet = getSegState(context);
      if (weightOrBitSet.weight != null) {
        return weightOrBitSet.weight.scorer(context);
      } else {
        return scorer(weightOrBitSet.set);
      }
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return true;
    }

  }
}







