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
package org.apache.lucene.search.join;

import static org.apache.lucene.search.ScoreMode.COMPLETE;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;

/**
 * This query requires that you index children and parent docs as a single block, using the {@link
 * IndexWriter#addDocuments IndexWriter.addDocuments()} or {@link IndexWriter#updateDocuments
 * IndexWriter.updateDocuments()} API. In each block, the child documents must appear first, ending
 * with the parent document. At search time you provide a Filter identifying the parents, however
 * this Filter must provide an {@link BitSet} per sub-reader.
 *
 * <p>Once the block index is built, use this query to wrap any sub-query matching only child docs
 * and join matches in that child document space up to the parent document space. You can then use
 * this Query as a clause with other queries in the parent document space.
 *
 * <p>See {@link ToChildBlockJoinQuery} if you need to join in the reverse order.
 *
 * <p>The child documents must be orthogonal to the parent documents: the wrapped child query must
 * never return a parent document.
 *
 * <p>See {@link org.apache.lucene.search.join} for an overview.
 *
 * @lucene.experimental
 */
public class ToParentBlockJoinQuery extends Query {

  private final BitSetProducer parentsFilter;
  private final Query childQuery;
  private final ScoreMode scoreMode;

  /**
   * Create a ToParentBlockJoinQuery.
   *
   * @param childQuery Query matching child documents.
   * @param parentsFilter Filter identifying the parent documents.
   * @param scoreMode How to aggregate multiple child scores into a single parent score.
   */
  public ToParentBlockJoinQuery(
      Query childQuery, BitSetProducer parentsFilter, ScoreMode scoreMode) {
    super();
    this.childQuery = childQuery;
    this.parentsFilter = parentsFilter;
    this.scoreMode = scoreMode;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public Weight createWeight(
      IndexSearcher searcher, org.apache.lucene.search.ScoreMode weightScoreMode, float boost)
      throws IOException {
    ScoreMode childScoreMode = weightScoreMode.needsScores() ? scoreMode : ScoreMode.None;
    final Weight childWeight;
    if (childScoreMode == ScoreMode.None) {
      // we don't need to compute a score for the child query so we wrap
      // it under a constant score query that can early terminate if the
      // minimum score is greater than 0 and the total hits that match the
      // query is not requested.
      childWeight =
          searcher
              .rewrite(new ConstantScoreQuery(childQuery))
              .createWeight(searcher, weightScoreMode, 0f);
    } else {
      // if the score is needed we force the collection mode to COMPLETE because the child query
      // cannot skip
      // non-competitive documents.
      childWeight =
          childQuery.createWeight(
              searcher, weightScoreMode.needsScores() ? COMPLETE : weightScoreMode, boost);
    }
    return new BlockJoinWeight(this, childWeight, parentsFilter, childScoreMode);
  }

  /** Return our child query. */
  public Query getChildQuery() {
    return childQuery;
  }

  private static class BlockJoinWeight extends FilterWeight {
    private final BitSetProducer parentsFilter;
    private final ScoreMode scoreMode;

    public BlockJoinWeight(
        Query joinQuery, Weight childWeight, BitSetProducer parentsFilter, ScoreMode scoreMode) {
      super(joinQuery, childWeight);
      this.parentsFilter = parentsFilter;
      this.scoreMode = scoreMode;
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      final ScorerSupplier scorerSupplier = scorerSupplier(context);
      if (scorerSupplier == null) {
        return null;
      }
      return scorerSupplier.get(Long.MAX_VALUE);
    }

    // NOTE: acceptDocs applies (and is checked) only in the
    // parent document space
    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
      final ScorerSupplier childScorerSupplier = in.scorerSupplier(context);
      if (childScorerSupplier == null) {
        return null;
      }

      // NOTE: this does not take accept docs into account, the responsibility
      // to not match deleted docs is on the scorer
      final BitSet parents = parentsFilter.getBitSet(context);
      if (parents == null) {
        // No matches
        return null;
      }

      return new ScorerSupplier() {

        @Override
        public Scorer get(long leadCost) throws IOException {
          return new BlockJoinScorer(
              BlockJoinWeight.this, childScorerSupplier.get(leadCost), parents, scoreMode);
        }

        @Override
        public long cost() {
          return childScorerSupplier.cost();
        }
      };
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      BlockJoinScorer scorer = (BlockJoinScorer) scorer(context);
      if (scorer != null && scorer.iterator().advance(doc) == doc) {
        return scorer.explain(context, in);
      }
      return Explanation.noMatch("Not a match");
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      // The default implementation would delegate to the joinQuery's Weight, which
      // matches on children.  We need to match on the parent instead
      Scorer scorer = scorer(context);
      if (scorer == null) {
        return null;
      }
      final TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
      if (twoPhase == null) {
        if (scorer.iterator().advance(doc) != doc) {
          return null;
        }
      } else {
        if (twoPhase.approximation().advance(doc) != doc || twoPhase.matches() == false) {
          return null;
        }
      }
      return MatchesUtils.MATCH_WITH_NO_TERMS;
    }
  }

  private static class ParentApproximation extends DocIdSetIterator {

    private final DocIdSetIterator childApproximation;
    private final BitSet parentBits;
    private int doc = -1;

    ParentApproximation(DocIdSetIterator childApproximation, BitSet parentBits) {
      this.childApproximation = childApproximation;
      this.parentBits = parentBits;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      if (target >= parentBits.length()) {
        return doc = NO_MORE_DOCS;
      }
      final int firstChildTarget = target == 0 ? 0 : parentBits.prevSetBit(target - 1) + 1;
      int childDoc = childApproximation.docID();
      if (childDoc < firstChildTarget) {
        childDoc = childApproximation.advance(firstChildTarget);
      }
      if (childDoc >= parentBits.length() - 1) {
        return doc = NO_MORE_DOCS;
      }
      return doc = parentBits.nextSetBit(childDoc + 1);
    }

    @Override
    public long cost() {
      return childApproximation.cost();
    }
  }

  private static class ParentTwoPhase extends TwoPhaseIterator {

    private final ParentApproximation parentApproximation;
    private final DocIdSetIterator childApproximation;
    private final TwoPhaseIterator childTwoPhase;

    ParentTwoPhase(ParentApproximation parentApproximation, TwoPhaseIterator childTwoPhase) {
      super(parentApproximation);
      this.parentApproximation = parentApproximation;
      this.childApproximation = childTwoPhase.approximation();
      this.childTwoPhase = childTwoPhase;
    }

    @Override
    public boolean matches() throws IOException {
      assert childApproximation.docID() < parentApproximation.docID();
      do {
        if (childTwoPhase.matches()) {
          return true;
        }
      } while (childApproximation.nextDoc() < parentApproximation.docID());
      return false;
    }

    @Override
    public float matchCost() {
      // TODO: how could we compute a match cost?
      return childTwoPhase.matchCost() + 10;
    }
  }

  static class BlockJoinScorer extends Scorer {
    private final Scorer childScorer;
    private final BitSet parentBits;
    private final ScoreMode scoreMode;
    private final DocIdSetIterator childApproximation;
    private final TwoPhaseIterator childTwoPhase;
    private final ParentApproximation parentApproximation;
    private final ParentTwoPhase parentTwoPhase;
    private float score;

    public BlockJoinScorer(
        Weight weight, Scorer childScorer, BitSet parentBits, ScoreMode scoreMode) {
      super(weight);
      // System.out.println("Q.init firstChildDoc=" + firstChildDoc);
      this.parentBits = parentBits;
      this.childScorer = childScorer;
      this.scoreMode = scoreMode;
      childTwoPhase = childScorer.twoPhaseIterator();
      if (childTwoPhase == null) {
        childApproximation = childScorer.iterator();
        parentApproximation = new ParentApproximation(childApproximation, parentBits);
        parentTwoPhase = null;
      } else {
        childApproximation = childTwoPhase.approximation();
        parentApproximation = new ParentApproximation(childTwoPhase.approximation(), parentBits);
        parentTwoPhase = new ParentTwoPhase(parentApproximation, childTwoPhase);
      }
    }

    @Override
    public Collection<ChildScorable> getChildren() {
      return Collections.singleton(new ChildScorable(childScorer, "BLOCK_JOIN"));
    }

    @Override
    public DocIdSetIterator iterator() {
      if (parentTwoPhase == null) {
        // the approximation is exact
        return parentApproximation;
      } else {
        return TwoPhaseIterator.asDocIdSetIterator(parentTwoPhase);
      }
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {
      return parentTwoPhase;
    }

    @Override
    public int docID() {
      return parentApproximation.docID();
    }

    @Override
    public float score() throws IOException {
      setScoreAndFreq();
      return score;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      if (scoreMode == ScoreMode.None) {
        return childScorer.getMaxScore(upTo);
      }
      return Float.POSITIVE_INFINITY;
    }

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
      if (scoreMode == ScoreMode.None) {
        childScorer.setMinCompetitiveScore(minScore);
      }
    }

    private void setScoreAndFreq() throws IOException {
      if (childApproximation.docID() >= parentApproximation.docID()) {
        return;
      }
      double score = scoreMode == ScoreMode.None ? 0 : childScorer.score();
      int freq = 1;
      while (childApproximation.nextDoc() < parentApproximation.docID()) {
        if (childTwoPhase == null || childTwoPhase.matches()) {
          final float childScore = scoreMode == ScoreMode.None ? 0 : childScorer.score();
          freq += 1;
          switch (scoreMode) {
            case Total:
            case Avg:
              score += childScore;
              break;
            case Min:
              score = Math.min(score, childScore);
              break;
            case Max:
              score = Math.max(score, childScore);
              break;
            case None:
              break;
            default:
              throw new AssertionError();
          }
        }
      }
      if (childApproximation.docID() == parentApproximation.docID()
          && (childTwoPhase == null || childTwoPhase.matches())) {
        throw new IllegalStateException(
            "Child query must not match same docs with parent filter. "
                + "Combine them as must clauses (+) to find a problem doc. "
                + "docId="
                + parentApproximation.docID()
                + ", "
                + childScorer.getClass());
      }
      if (scoreMode == ScoreMode.Avg) {
        score /= freq;
      }
      this.score = (float) score;
    }

    public Explanation explain(LeafReaderContext context, Weight childWeight) throws IOException {
      int prevParentDoc = parentBits.prevSetBit(parentApproximation.docID() - 1);
      int start =
          context.docBase + prevParentDoc + 1; // +1 b/c prevParentDoc is previous parent doc
      int end = context.docBase + parentApproximation.docID() - 1; // -1 b/c parentDoc is parent doc

      Explanation bestChild = null;
      int matches = 0;
      for (int childDoc = start; childDoc <= end; childDoc++) {
        Explanation child = childWeight.explain(context, childDoc - context.docBase);
        if (child.isMatch()) {
          matches++;
          if (bestChild == null
              || child.getValue().floatValue() > bestChild.getValue().floatValue()) {
            bestChild = child;
          }
        }
      }

      return Explanation.match(
          score(),
          String.format(
              Locale.ROOT,
              "Score based on %d child docs in range from %d to %d, best match:",
              matches,
              start,
              end),
          bestChild);
    }
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final Query childRewrite = childQuery.rewrite(reader);
    if (childRewrite != childQuery) {
      return new ToParentBlockJoinQuery(childRewrite, parentsFilter, scoreMode);
    } else {
      return super.rewrite(reader);
    }
  }

  @Override
  public String toString(String field) {
    return "ToParentBlockJoinQuery (" + childQuery.toString() + ")";
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(ToParentBlockJoinQuery other) {
    return childQuery.equals(other.childQuery)
        && parentsFilter.equals(other.parentsFilter)
        && scoreMode == other.scoreMode;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int hash = classHash();
    hash = prime * hash + childQuery.hashCode();
    hash = prime * hash + scoreMode.hashCode();
    hash = prime * hash + parentsFilter.hashCode();
    return hash;
  }
}
