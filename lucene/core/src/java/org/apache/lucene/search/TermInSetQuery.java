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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.PrefixCodedTerms.TermIterator;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Specialization for a disjunction over many terms that behaves like a
 * {@link ConstantScoreQuery} over a {@link BooleanQuery} containing only
 * {@link org.apache.lucene.search.BooleanClause.Occur#SHOULD} clauses.
 * <p>For instance in the following example, both @{code q1} and {@code q2}
 * would yield the same scores:
 * <pre class="prettyprint">
 * Query q1 = new TermInSetQuery(new Term("field", "foo"), new Term("field", "bar"));
 *
 * BooleanQuery bq = new BooleanQuery();
 * bq.add(new TermQuery(new Term("field", "foo")), Occur.SHOULD);
 * bq.add(new TermQuery(new Term("field", "bar")), Occur.SHOULD);
 * Query q2 = new ConstantScoreQuery(bq);
 * </pre>
 * <p>When there are few terms, this query executes like a regular disjunction.
 * However, when there are many terms, instead of merging iterators on the fly,
 * it will populate a bit set with matching docs and return a {@link Scorer}
 * over this bit set.
 * <p>NOTE: This query produces scores that are equal to its boost
 */
public class TermInSetQuery extends Query implements Accountable {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TermInSetQuery.class);
  // Same threshold as MultiTermQueryConstantScoreWrapper
  static final int BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD = 16;

  private final String field;
  private final PrefixCodedTerms termData;
  private final int termDataHashCode; // cached hashcode of termData

  /**
   * Creates a new {@link TermInSetQuery} from the given collection of terms.
   */
  public TermInSetQuery(String field, Collection<BytesRef> terms) {
    BytesRef[] sortedTerms = terms.toArray(new BytesRef[terms.size()]);
    // already sorted if we are a SortedSet with natural order
    boolean sorted = terms instanceof SortedSet && ((SortedSet<BytesRef>)terms).comparator() == null;
    if (!sorted) {
      ArrayUtil.timSort(sortedTerms);
    }
    PrefixCodedTerms.Builder builder = new PrefixCodedTerms.Builder();
    BytesRefBuilder previous = null;
    for (BytesRef term : sortedTerms) {
      if (previous == null) {
        previous = new BytesRefBuilder();
      } else if (previous.get().equals(term)) {
        continue; // deduplicate
      }
      builder.add(field, term);
      previous.copyBytes(term);
    }
    this.field = field;
    termData = builder.finish();
    termDataHashCode = termData.hashCode();
  }

  /**
   * Creates a new {@link TermInSetQuery} from the given array of terms.
   */
  public TermInSetQuery(String field, BytesRef...terms) {
    this(field, Arrays.asList(terms));
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final int threshold = Math.min(BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD, BooleanQuery.getMaxClauseCount());
    if (termData.size() <= threshold) {
      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      TermIterator iterator = termData.iterator();
      for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
        bq.add(new TermQuery(new Term(iterator.field(), BytesRef.deepCopyOf(term))), Occur.SHOULD);
      }
      return new ConstantScoreQuery(bq.build());
    }
    return super.rewrite(reader);
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
        equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(TermInSetQuery other) {
    // no need to check 'field' explicitly since it is encoded in 'termData'
    // termData might be heavy to compare so check the hash code first
    return termDataHashCode == other.termDataHashCode &&
        termData.equals(other.termData);
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + termDataHashCode;
  }

  /** Returns the terms wrapped in a PrefixCodedTerms. */
  public PrefixCodedTerms getTermData() {
    return termData;
  }

  @Override
  public String toString(String defaultField) {
    StringBuilder builder = new StringBuilder();
    builder.append(field);
    builder.append(":(");

    TermIterator iterator = termData.iterator();
    boolean first = true;
    for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
      if (!first) {
        builder.append(' ');
      }
      first = false;
      builder.append(Term.toString(term));
    }
    builder.append(')');

    return builder.toString();
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + termData.ramBytesUsed();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

  private static class TermAndState {
    final String field;
    final TermsEnum termsEnum;
    final BytesRef term;
    final TermState state;
    final int docFreq;
    final long totalTermFreq;

    TermAndState(String field, TermsEnum termsEnum) throws IOException {
      this.field = field;
      this.termsEnum = termsEnum;
      this.term = BytesRef.deepCopyOf(termsEnum.term());
      this.state = termsEnum.termState();
      this.docFreq = termsEnum.docFreq();
      this.totalTermFreq = termsEnum.totalTermFreq();
    }
  }

  private static class WeightOrDocIdSet {
    final Weight weight;
    final DocIdSet set;

    WeightOrDocIdSet(Weight weight) {
      this.weight = Objects.requireNonNull(weight);
      this.set = null;
    }

    WeightOrDocIdSet(DocIdSet bitset) {
      this.set = bitset;
      this.weight = null;
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    return new ConstantScoreWeight(this, boost) {

      @Override
      public void extractTerms(Set<Term> terms) {
        // no-op
        // This query is for abuse cases when the number of terms is too high to
        // run efficiently as a BooleanQuery. So likewise we hide its terms in
        // order to protect highlighters
      }

      @Override
      public Matches matches(LeafReaderContext context, int doc) throws IOException {
        Terms terms = context.reader().terms(field);
        if (terms == null || terms.hasPositions() == false) {
          return super.matches(context, doc);
        }
        return Matches.forField(field, () -> DisjunctionMatchesIterator.fromTermsEnum(context, doc, field, termData.iterator()));
      }

      /**
       * On the given leaf context, try to either rewrite to a disjunction if
       * there are few matching terms, or build a bitset containing matching docs.
       */
      private WeightOrDocIdSet rewrite(LeafReaderContext context) throws IOException {
        final LeafReader reader = context.reader();

        Terms terms = reader.terms(field);
        if (terms == null) {
          return null;
        }
        TermsEnum termsEnum = terms.iterator();
        PostingsEnum docs = null;
        TermIterator iterator = termData.iterator();

        // We will first try to collect up to 'threshold' terms into 'matchingTerms'
        // if there are two many terms, we will fall back to building the 'builder'
        final int threshold = Math.min(BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD, BooleanQuery.getMaxClauseCount());
        assert termData.size() > threshold : "Query should have been rewritten";
        List<TermAndState> matchingTerms = new ArrayList<>(threshold);
        DocIdSetBuilder builder = null;

        for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
          assert field.equals(iterator.field());
          if (termsEnum.seekExact(term)) {
            if (matchingTerms == null) {
              docs = termsEnum.postings(docs, PostingsEnum.NONE);
              builder.add(docs);
            } else if (matchingTerms.size() < threshold) {
              matchingTerms.add(new TermAndState(field, termsEnum));
            } else {
              assert matchingTerms.size() == threshold;
              builder = new DocIdSetBuilder(reader.maxDoc(), terms);
              docs = termsEnum.postings(docs, PostingsEnum.NONE);
              builder.add(docs);
              for (TermAndState t : matchingTerms) {
                t.termsEnum.seekExact(t.term, t.state);
                docs = t.termsEnum.postings(docs, PostingsEnum.NONE);
                builder.add(docs);
              }
              matchingTerms = null;
            }
          }
        }
        if (matchingTerms != null) {
          assert builder == null;
          BooleanQuery.Builder bq = new BooleanQuery.Builder();
          for (TermAndState t : matchingTerms) {
            final TermContext termContext = new TermContext(searcher.getTopReaderContext());
            termContext.register(t.state, context.ord, t.docFreq, t.totalTermFreq);
            bq.add(new TermQuery(new Term(t.field, t.term), termContext), Occur.SHOULD);
          }
          Query q = new ConstantScoreQuery(bq.build());
          final Weight weight = searcher.rewrite(q).createWeight(searcher, needsScores, score());
          return new WeightOrDocIdSet(weight);
        } else {
          assert builder != null;
          return new WeightOrDocIdSet(builder.build());
        }
      }

      private Scorer scorer(DocIdSet set) throws IOException {
        if (set == null) {
          return null;
        }
        final DocIdSetIterator disi = set.iterator();
        if (disi == null) {
          return null;
        }
        return new ConstantScoreScorer(this, score(), disi);
      }

      @Override
      public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
        final WeightOrDocIdSet weightOrBitSet = rewrite(context);
        if (weightOrBitSet == null) {
          return null;
        } else if (weightOrBitSet.weight != null) {
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
        final WeightOrDocIdSet weightOrBitSet = rewrite(context);
        if (weightOrBitSet == null) {
          return null;
        } else if (weightOrBitSet.weight != null) {
          return weightOrBitSet.weight.scorer(context);
        } else {
          return scorer(weightOrBitSet.set);
        }
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        // Only cache instances that have a reasonable size. Otherwise it might cause memory issues
        // with the query cache if most memory ends up being spent on queries rather than doc id sets.
        return ramBytesUsed() <= LRUQueryCache.QUERY_DEFAULT_RAM_BYTES_USED;
      }

    };
  }
}
