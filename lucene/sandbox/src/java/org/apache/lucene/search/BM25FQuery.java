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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.SimilarityBase;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * A {@link Query} that treats multiple fields as a single stream and scores
 * terms as if you had indexed them as a single term in a single field.
 *
 * For scoring purposes this query implements the BM25F's simple formula
 * described in:
 *  http://www.staff.city.ac.uk/~sb317/papers/foundations_bm25_review.pdf
 *
 * The per-field similarity is ignored but to be compatible each field must use
 * a {@link Similarity} at index time that encodes norms the same way as
 * {@link SimilarityBase#computeNorm}.
 *
 * @lucene.experimental
 */
public final class BM25FQuery extends Query implements Accountable {
  private static final long BASE_RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(BM25FQuery.class);

  /**
   * A builder for {@link BM25FQuery}.
   */
  public static class Builder {
    private final BM25Similarity similarity;
    private final Map<String, FieldAndWeight> fieldAndWeights = new HashMap<>();
    private final Set<BytesRef> termsSet = new HashSet<>();

    /**
     * Default builder.
     */
    public Builder() {
      this.similarity = new BM25Similarity();
    }

    /**
     * Builder with the supplied parameter values.
     * @param k1 Controls non-linear term frequency normalization (saturation).
     * @param b Controls to what degree document length normalizes tf values.
     */
    public Builder(float k1, float b) {
      this.similarity = new BM25Similarity(k1, b);
    }

    /**
     * Adds a field to this builder.
     * @param field The field name.
     */
    public Builder addField(String field) {
      return addField(field, 1f);
    }

    /**
     * Adds a field to this builder.
     * @param field The field name.
     * @param weight The weight associated to this field.
     */
    public Builder addField(String field, float weight) {
      if (weight < 1) {
        throw new IllegalArgumentException("weight must be greater or equal to 1");
      }
      fieldAndWeights.put(field, new FieldAndWeight(field, weight));
      return this;
    }

    /**
     * Adds a term to this builder.
     */
    public Builder addTerm(BytesRef term) {
      if (termsSet.size() > IndexSearcher.getMaxClauseCount()) {
        throw new IndexSearcher.TooManyClauses();
      }
      termsSet.add(term);
      return this;
    }

    /**
     * Builds the {@link BM25FQuery}.
     */
    public BM25FQuery build() {
      int size = fieldAndWeights.size() * termsSet.size();
      if (size > IndexSearcher.getMaxClauseCount()) {
        throw new IndexSearcher.TooManyClauses();
      }
      BytesRef[] terms = termsSet.toArray(new BytesRef[0]);
      return new BM25FQuery(similarity, new TreeMap<>(fieldAndWeights), terms);
    }
  }

  static class FieldAndWeight {
    final String field;
    final float weight;

    FieldAndWeight(String field, float weight) {
      this.field = field;
      this.weight = weight;
    }
  }

  // the similarity to use for scoring.
  private final BM25Similarity similarity;
  // sorted map for fields.
  private final TreeMap<String, FieldAndWeight> fieldAndWeights;
  // array of terms, sorted.
  private final BytesRef terms[];
  // array of terms per field, sorted
  private final Term fieldTerms[];

  private final long ramBytesUsed;

  private BM25FQuery(BM25Similarity similarity, TreeMap<String, FieldAndWeight> fieldAndWeights, BytesRef[] terms) {
    this.similarity = similarity;
    this.fieldAndWeights = fieldAndWeights;
    this.terms = terms;
    int numFieldTerms = fieldAndWeights.size() * terms.length;
    if (numFieldTerms > IndexSearcher.getMaxClauseCount()) {
      throw new IndexSearcher.TooManyClauses();
    }
    this.fieldTerms = new Term[numFieldTerms];
    Arrays.sort(terms);
    int pos = 0;
    for (String field : fieldAndWeights.keySet()) {
      for (BytesRef term : terms) {
        fieldTerms[pos++] = new Term(field, term);
      }
    }

    this.ramBytesUsed = BASE_RAM_BYTES +
        RamUsageEstimator.sizeOfObject(fieldAndWeights) +
        RamUsageEstimator.sizeOfObject(fieldTerms) +
        RamUsageEstimator.sizeOfObject(terms);
  }

  public List<Term> getTerms() {
    return Collections.unmodifiableList(Arrays.asList(fieldTerms));
  }

  @Override
  public String toString(String field) {
    StringBuilder builder = new StringBuilder("BM25F((");
    int pos = 0;
    for (FieldAndWeight fieldWeight : fieldAndWeights.values()) {
      if (pos++ != 0) {
        builder.append(" ");
      }
      builder.append(fieldWeight.field);
      if (fieldWeight.weight != 1f) {
        builder.append("^");
        builder.append(fieldWeight.weight);
      }
    }
    builder.append(")(");
    pos = 0;
    for (BytesRef term : terms) {
      if (pos++ != 0) {
        builder.append(" ");
      }
      builder.append(term.utf8ToString());
    }
    builder.append("))");
    return builder.toString();
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + Arrays.hashCode(terms);
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
        Arrays.equals(terms, ((BM25FQuery) other).terms);
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    // optimize zero and single field cases
    if (terms.length == 0) {
      return new BooleanQuery.Builder().build();
    }
    // single field and one term
    if (fieldTerms.length == 1) {
      return new TermQuery(fieldTerms[0]);
    }
    // single field and multiple terms
    if (fieldAndWeights.size() == 1) {
      SynonymQuery.Builder builder = new SynonymQuery.Builder(fieldTerms[0].field());
      for (Term term : fieldTerms) {
        builder.addTerm(term);
      }
      return builder.build();
    }
    return this;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    Term[] selectedTerms = Arrays.stream(fieldTerms).filter(t -> visitor.acceptField(t.field())).toArray(Term[]::new);
    if (selectedTerms.length > 0) {
      QueryVisitor v = visitor.getSubVisitor(BooleanClause.Occur.SHOULD, this);
      v.consumeTerms(this, selectedTerms);
    }
  }

  private BooleanQuery rewriteToBoolean() {
    // rewrite to a simple disjunction if the score is not needed.
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    for (Term term : fieldTerms) {
      bq.add(new TermQuery(term), BooleanClause.Occur.SHOULD);
    }
    return bq.build();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    if (scoreMode.needsScores()) {
      return new BM25FWeight(this, searcher, scoreMode, boost);
    } else {
      // rewrite to a simple disjunction if the score is not needed.
      Query bq = rewriteToBoolean();
      return searcher.rewrite(bq).createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, boost);
    }
  }

  class BM25FWeight extends Weight {
    private final IndexSearcher searcher;
    private final TermStates termStates[];
    private final Similarity.SimScorer simWeight;

    BM25FWeight(Query query, IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
      super(query);
      assert scoreMode.needsScores();
      this.searcher = searcher;
      long docFreq = 0;
      long totalTermFreq = 0;
      termStates = new TermStates[fieldTerms.length];
      for (int i = 0; i < termStates.length; i++) {
        FieldAndWeight field = fieldAndWeights.get(fieldTerms[i].field());
        TermStates ts = TermStates.build(searcher.getTopReaderContext(), fieldTerms[i], true);
        termStates[i] = ts;
        if (ts.docFreq() > 0) {
          TermStatistics termStats = searcher.termStatistics(fieldTerms[i], ts.docFreq(), ts.totalTermFreq());
          docFreq = Math.max(termStats.docFreq(), docFreq);
          totalTermFreq += (double) field.weight * termStats.totalTermFreq();
        }
      }
      if (docFreq > 0) {
        CollectionStatistics pseudoCollectionStats = mergeCollectionStatistics(searcher);
        TermStatistics pseudoTermStatistics = new TermStatistics(new BytesRef("pseudo_term"), docFreq, Math.max(1, totalTermFreq));
        this.simWeight = similarity.scorer(boost, pseudoCollectionStats, pseudoTermStatistics);
      } else {
        this.simWeight = null;
      }
    }

    private CollectionStatistics mergeCollectionStatistics(IndexSearcher searcher) throws IOException {
      long maxDoc = searcher.getIndexReader().maxDoc();
      long docCount = 0;
      long sumTotalTermFreq = 0;
      long sumDocFreq = 0;
      for (FieldAndWeight fieldWeight : fieldAndWeights.values()) {
        CollectionStatistics collectionStats = searcher.collectionStatistics(fieldWeight.field);
        if (collectionStats != null) {
          docCount = Math.max(collectionStats.docCount(), docCount);
          sumDocFreq = Math.max(collectionStats.sumDocFreq(), sumDocFreq);
          sumTotalTermFreq += (double) fieldWeight.weight * collectionStats.sumTotalTermFreq();
        }
      }

      return new CollectionStatistics("pseudo_field", maxDoc, docCount, sumTotalTermFreq, sumDocFreq);
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      Weight weight = searcher.rewrite(rewriteToBoolean()).createWeight(searcher, ScoreMode.COMPLETE, 1f);
      return weight.matches(context, doc);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      Scorer scorer = scorer(context);
      if (scorer != null) {
        int newDoc = scorer.iterator().advance(doc);
        if (newDoc == doc) {
          final float freq;
          if (scorer instanceof BM25FScorer) {
            freq = ((BM25FScorer) scorer).freq();
          } else {
            assert scorer instanceof TermScorer;
            freq = ((TermScorer) scorer).freq();
          }
          final MultiNormsLeafSimScorer docScorer =
              new MultiNormsLeafSimScorer(simWeight, context.reader(), fieldAndWeights.values(), true);
          Explanation freqExplanation = Explanation.match(freq, "termFreq=" + freq);
          Explanation scoreExplanation = docScorer.explain(doc, freqExplanation);
          return Explanation.match(
              scoreExplanation.getValue(),
              "weight(" + getQuery() + " in " + doc + ") ["
                  + similarity.getClass().getSimpleName() + "], result of:",
              scoreExplanation);
        }
      }
      return Explanation.noMatch("no matching term");
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      List<PostingsEnum> iterators = new ArrayList<>();
      List<FieldAndWeight> fields = new ArrayList<>();
      for (int i = 0; i < fieldTerms.length; i++) {
        TermState state = termStates[i].get(context);
        if (state != null) {
          TermsEnum termsEnum = context.reader().terms(fieldTerms[i].field()).iterator();
          termsEnum.seekExact(fieldTerms[i].bytes(), state);
          PostingsEnum postingsEnum = termsEnum.postings(null, PostingsEnum.FREQS);
          iterators.add(postingsEnum);
          fields.add(fieldAndWeights.get(fieldTerms[i].field()));
        }
      }

      if (iterators.isEmpty()) {
        return null;
      }

      // we must optimize this case (term not in segment), disjunctions require >= 2 subs
      if (iterators.size() == 1) {
        final LeafSimScorer scoringSimScorer =
            new LeafSimScorer(simWeight, context.reader(), fields.get(0).field, true);
        return new TermScorer(this, iterators.get(0), scoringSimScorer);
      }
      final MultiNormsLeafSimScorer scoringSimScorer =
          new MultiNormsLeafSimScorer(simWeight, context.reader(), fields, true);
      LeafSimScorer nonScoringSimScorer = new LeafSimScorer(simWeight, context.reader(), "pseudo_field", false);
      // we use termscorers + disjunction as an impl detail
      DisiPriorityQueue queue = new DisiPriorityQueue(iterators.size());
      for (int i = 0; i < iterators.size(); i++) {
        float weight = fields.get(i).weight;
        queue.add(new WeightedDisiWrapper(new TermScorer(this, iterators.get(i), nonScoringSimScorer), weight));
      }
      // Even though it is called approximation, it is accurate since none of
      // the sub iterators are two-phase iterators.
      DocIdSetIterator iterator = new DisjunctionDISIApproximation(queue);
      return new BM25FScorer(this, queue, iterator, scoringSimScorer);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

  private static class WeightedDisiWrapper extends DisiWrapper {
    final float weight;

    WeightedDisiWrapper(Scorer scorer, float weight) {
      super(scorer);
      this.weight = weight;
    }

    float freq() throws IOException {
      return weight * ((PostingsEnum) iterator).freq();
    }
  }

  private static class BM25FScorer extends Scorer {
    private final DisiPriorityQueue queue;
    private final DocIdSetIterator iterator;
    private final MultiNormsLeafSimScorer simScorer;

    BM25FScorer(Weight weight, DisiPriorityQueue queue, DocIdSetIterator iterator, MultiNormsLeafSimScorer simScorer) {
      super(weight);
      this.queue = queue;
      this.iterator = iterator;
      this.simScorer = simScorer;
    }

    @Override
    public int docID() {
      return iterator.docID();
    }

    float freq() throws IOException {
      DisiWrapper w = queue.topList();
      float freq = ((WeightedDisiWrapper) w).freq();
      for (w = w.next; w != null; w = w.next) {
        freq += ((WeightedDisiWrapper) w).freq();
        if (freq < 0) { // overflow
          return Integer.MAX_VALUE;
        }
      }
      return freq;
    }

    @Override
    public float score() throws IOException {
      return simScorer.score(iterator.docID(), freq());
    }

    @Override
    public DocIdSetIterator iterator() {
      return iterator;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return Float.POSITIVE_INFINITY;
    }
  }
}