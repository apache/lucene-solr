package org.apache.lucene.search.join;

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
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;       // javadocs
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

/**
 * This query requires that you index
 * children and parent docs as a single block, using the
 * {@link IndexWriter#addDocuments IndexWriter.addDocuments()} or {@link
 * IndexWriter#updateDocuments IndexWriter.updateDocuments()} API.  In each block, the
 * child documents must appear first, ending with the parent
 * document.  At search time you provide a Filter
 * identifying the parents, however this Filter must provide
 * an {@link FixedBitSet} per sub-reader.
 *
 * <p>Once the block index is built, use this query to wrap
 * any sub-query matching only child docs and join matches in that
 * child document space up to the parent document space.
 * You can then use this Query as a clause with
 * other queries in the parent document space.</p>
 *
 * <p>See {@link ToChildBlockJoinQuery} if you need to join
 * in the reverse order.
 *
 * <p>The child documents must be orthogonal to the parent
 * documents: the wrapped child query must never
 * return a parent document.</p>
 *
 * If you'd like to retrieve {@link TopGroups} for the
 * resulting query, use the {@link ToParentBlockJoinCollector}.
 * Note that this is not necessary, ie, if you simply want
 * to collect the parent documents and don't need to see
 * which child documents matched under that parent, then
 * you can use any collector.
 *
 * <p><b>NOTE</b>: If the overall query contains parent-only
 * matches, for example you OR a parent-only query with a
 * joined child-only query, then the resulting collected documents
 * will be correct, however the {@link TopGroups} you get
 * from {@link ToParentBlockJoinCollector} will not contain every
 * child for parents that had matched.
 *
 * <p>See {@link org.apache.lucene.search.join} for an
 * overview. </p>
 *
 * @lucene.experimental
 */
public class ToParentBlockJoinQuery extends Query {

  private final Filter parentsFilter;
  private final Query childQuery;

  // If we are rewritten, this is the original childQuery we
  // were passed; we use this for .equals() and
  // .hashCode().  This makes rewritten query equal the
  // original, so that user does not have to .rewrite() their
  // query before searching:
  private final Query origChildQuery;
  private final ScoreMode scoreMode;

  /** Create a ToParentBlockJoinQuery.
   * 
   * @param childQuery Query matching child documents.
   * @param parentsFilter Filter (must produce FixedBitSet
   * per-segment) identifying the parent documents.
   * @param scoreMode How to aggregate multiple child scores
   * into a single parent score.
   **/
  public ToParentBlockJoinQuery(Query childQuery, Filter parentsFilter, ScoreMode scoreMode) {
    super();
    this.origChildQuery = childQuery;
    this.childQuery = childQuery;
    this.parentsFilter = parentsFilter;
    this.scoreMode = scoreMode;
  }

  private ToParentBlockJoinQuery(Query origChildQuery, Query childQuery, Filter parentsFilter, ScoreMode scoreMode) {
    super();
    this.origChildQuery = origChildQuery;
    this.childQuery = childQuery;
    this.parentsFilter = parentsFilter;
    this.scoreMode = scoreMode;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new BlockJoinWeight(this, childQuery.createWeight(searcher), parentsFilter, scoreMode);
  }

  private static class BlockJoinWeight extends Weight {
    private final Query joinQuery;
    private final Weight childWeight;
    private final Filter parentsFilter;
    private final ScoreMode scoreMode;

    public BlockJoinWeight(Query joinQuery, Weight childWeight, Filter parentsFilter, ScoreMode scoreMode) {
      super();
      this.joinQuery = joinQuery;
      this.childWeight = childWeight;
      this.parentsFilter = parentsFilter;
      this.scoreMode = scoreMode;
    }

    @Override
    public Query getQuery() {
      return joinQuery;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return childWeight.getValueForNormalization() * joinQuery.getBoost() * joinQuery.getBoost();
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      childWeight.normalize(norm, topLevelBoost * joinQuery.getBoost());
    }

    // NOTE: acceptDocs applies (and is checked) only in the
    // parent document space
    @Override
    public Scorer scorer(AtomicReaderContext readerContext, boolean scoreDocsInOrder,
        boolean topScorer, Bits acceptDocs) throws IOException {

      // Pass scoreDocsInOrder true, topScorer false to our sub and the live docs:
      final Scorer childScorer = childWeight.scorer(readerContext, true, false, readerContext.reader().getLiveDocs());

      if (childScorer == null) {
        // No matches
        return null;
      }

      final int firstChildDoc = childScorer.nextDoc();
      if (firstChildDoc == DocIdSetIterator.NO_MORE_DOCS) {
        // No matches
        return null;
      }

      // NOTE: we cannot pass acceptDocs here because this
      // will (most likely, justifiably) cause the filter to
      // not return a FixedBitSet but rather a
      // BitsFilteredDocIdSet.  Instead, we filter by
      // acceptDocs when we score:
      final DocIdSet parents = parentsFilter.getDocIdSet(readerContext, null);

      if (parents == null || parents == DocIdSet.EMPTY_DOCIDSET) {
        // No matches
        return null;
      }
      if (!(parents instanceof FixedBitSet)) {
        throw new IllegalStateException("parentFilter must return FixedBitSet; got " + parents);
      }

      return new BlockJoinScorer(this, childScorer, (FixedBitSet) parents, firstChildDoc, scoreMode, acceptDocs);
    }

    @Override
    public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
      BlockJoinScorer scorer = (BlockJoinScorer) scorer(context, true, false, context.reader().getLiveDocs());
      if (scorer != null && scorer.advance(doc) == doc) {
        return scorer.explain(context.docBase);
      }
      return new ComplexExplanation(false, 0.0f, "Not a match");
    }

    @Override
    public boolean scoresDocsOutOfOrder() {
      return false;
    }
  }

  static class BlockJoinScorer extends Scorer {
    private final Scorer childScorer;
    private final FixedBitSet parentBits;
    private final ScoreMode scoreMode;
    private final Bits acceptDocs;
    private int parentDoc = -1;
    private int prevParentDoc;
    private float parentScore;
    private int parentFreq;
    private int nextChildDoc;

    private int[] pendingChildDocs = new int[5];
    private float[] pendingChildScores;
    private int childDocUpto;

    public BlockJoinScorer(Weight weight, Scorer childScorer, FixedBitSet parentBits, int firstChildDoc, ScoreMode scoreMode, Bits acceptDocs) {
      super(weight);
      //System.out.println("Q.init firstChildDoc=" + firstChildDoc);
      this.parentBits = parentBits;
      this.childScorer = childScorer;
      this.scoreMode = scoreMode;
      this.acceptDocs = acceptDocs;
      if (scoreMode != ScoreMode.None) {
        pendingChildScores = new float[5];
      }
      nextChildDoc = firstChildDoc;
    }

    @Override
    public Collection<ChildScorer> getChildren() {
      return Collections.singleton(new ChildScorer(childScorer, "BLOCK_JOIN"));
    }

    int getChildCount() {
      return childDocUpto;
    }

    int getParentDoc() {
      return parentDoc;
    }

    int[] swapChildDocs(int[] other) {
      final int[] ret = pendingChildDocs;
      if (other == null) {
        pendingChildDocs = new int[5];
      } else {
        pendingChildDocs = other;
      }
      return ret;
    }

    float[] swapChildScores(float[] other) {
      if (scoreMode == ScoreMode.None) {
        throw new IllegalStateException("ScoreMode is None; you must pass trackScores=false to ToParentBlockJoinCollector");
      }
      final float[] ret = pendingChildScores;
      if (other == null) {
        pendingChildScores = new float[5];
      } else {
        pendingChildScores = other;
      }
      return ret;
    }

    @Override
    public int nextDoc() throws IOException {
      //System.out.println("Q.nextDoc() nextChildDoc=" + nextChildDoc);
      // Loop until we hit a parentDoc that's accepted
      while (true) {
        if (nextChildDoc == NO_MORE_DOCS) {
          //System.out.println("  end");
          return parentDoc = NO_MORE_DOCS;
        }

        // Gather all children sharing the same parent as
        // nextChildDoc

        parentDoc = parentBits.nextSetBit(nextChildDoc);

        // Parent & child docs are supposed to be
        // orthogonal:
        if (nextChildDoc == parentDoc) {
          throw new IllegalStateException("child query must only match non-parent docs, but parent docID=" + nextChildDoc + " matched childScorer=" + childScorer.getClass());
        }

        //System.out.println("  parentDoc=" + parentDoc);
        assert parentDoc != -1;

        //System.out.println("  nextChildDoc=" + nextChildDoc);
        if (acceptDocs != null && !acceptDocs.get(parentDoc)) {
          // Parent doc not accepted; skip child docs until
          // we hit a new parent doc:
          do {
            nextChildDoc = childScorer.nextDoc();
          } while (nextChildDoc < parentDoc);

          // Parent & child docs are supposed to be
          // orthogonal:
          if (nextChildDoc == parentDoc) {
            throw new IllegalStateException("child query must only match non-parent docs, but parent docID=" + nextChildDoc + " matched childScorer=" + childScorer.getClass());
          }

          continue;
        }

        float totalScore = 0;
        float maxScore = Float.NEGATIVE_INFINITY;

        childDocUpto = 0;
        parentFreq = 0;
        do {

          //System.out.println("  c=" + nextChildDoc);
          if (pendingChildDocs.length == childDocUpto) {
            pendingChildDocs = ArrayUtil.grow(pendingChildDocs);
          }
          if (scoreMode != ScoreMode.None && pendingChildScores.length == childDocUpto) {
            pendingChildScores = ArrayUtil.grow(pendingChildScores);
          }
          pendingChildDocs[childDocUpto] = nextChildDoc;
          if (scoreMode != ScoreMode.None) {
            // TODO: specialize this into dedicated classes per-scoreMode
            final float childScore = childScorer.score();
            final int childFreq = childScorer.freq();
            pendingChildScores[childDocUpto] = childScore;
            maxScore = Math.max(childScore, maxScore);
            totalScore += childScore;
            parentFreq += childFreq;
          }
          childDocUpto++;
          nextChildDoc = childScorer.nextDoc();
        } while (nextChildDoc < parentDoc);

        // Parent & child docs are supposed to be
        // orthogonal:
        if (nextChildDoc == parentDoc) {
          throw new IllegalStateException("child query must only match non-parent docs, but parent docID=" + nextChildDoc + " matched childScorer=" + childScorer.getClass());
        }

        switch(scoreMode) {
        case Avg:
          parentScore = totalScore / childDocUpto;
          break;
        case Max:
          parentScore = maxScore;
          break;
        case Total:
          parentScore = totalScore;
          break;
        case None:
          break;
        }

        //System.out.println("  return parentDoc=" + parentDoc + " childDocUpto=" + childDocUpto);
        return parentDoc;
      }
    }

    @Override
    public int docID() {
      return parentDoc;
    }

    @Override
    public float score() throws IOException {
      return parentScore;
    }
    
    @Override
    public int freq() {
      return parentFreq;
    }

    @Override
    public int advance(int parentTarget) throws IOException {

      //System.out.println("Q.advance parentTarget=" + parentTarget);
      if (parentTarget == NO_MORE_DOCS) {
        return parentDoc = NO_MORE_DOCS;
      }

      if (parentTarget == 0) {
        // Callers should only be passing in a docID from
        // the parent space, so this means this parent
        // has no children (it got docID 0), so it cannot
        // possibly match.  We must handle this case
        // separately otherwise we pass invalid -1 to
        // prevSetBit below:
        return nextDoc();
      }

      prevParentDoc = parentBits.prevSetBit(parentTarget-1);

      //System.out.println("  rolled back to prevParentDoc=" + prevParentDoc + " vs parentDoc=" + parentDoc);
      assert prevParentDoc >= parentDoc;
      if (prevParentDoc > nextChildDoc) {
        nextChildDoc = childScorer.advance(prevParentDoc);
        // System.out.println("  childScorer advanced to child docID=" + nextChildDoc);
      //} else {
        //System.out.println("  skip childScorer advance");
      }

      // Parent & child docs are supposed to be orthogonal:
      if (nextChildDoc == prevParentDoc) {
        throw new IllegalStateException("child query must only match non-parent docs, but parent docID=" + nextChildDoc + " matched childScorer=" + childScorer.getClass());
      }

      final int nd = nextDoc();
      //System.out.println("  return nextParentDoc=" + nd);
      return nd;
    }

    public Explanation explain(int docBase) throws IOException {
      int start = docBase + prevParentDoc + 1; // +1 b/c prevParentDoc is previous parent doc
      int end = docBase + parentDoc - 1; // -1 b/c parentDoc is parent doc
      return new ComplexExplanation(
          true, score(), String.format(Locale.ROOT, "Score based on child doc range from %d to %d", start, end)
      );
    }

    @Override
    public long cost() {
      return childScorer.cost();
    }
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    childQuery.extractTerms(terms);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final Query childRewrite = childQuery.rewrite(reader);
    if (childRewrite != childQuery) {
      Query rewritten = new ToParentBlockJoinQuery(childQuery,
                                childRewrite,
                                parentsFilter,
                                scoreMode);
      rewritten.setBoost(getBoost());
      return rewritten;
    } else {
      return this;
    }
  }

  @Override
  public String toString(String field) {
    return "ToParentBlockJoinQuery ("+childQuery.toString()+")";
  }

  @Override
  public boolean equals(Object _other) {
    if (_other instanceof ToParentBlockJoinQuery) {
      final ToParentBlockJoinQuery other = (ToParentBlockJoinQuery) _other;
      return origChildQuery.equals(other.origChildQuery) &&
        parentsFilter.equals(other.parentsFilter) &&
        scoreMode == other.scoreMode && 
        super.equals(other);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int hash = super.hashCode();
    hash = prime * hash + origChildQuery.hashCode();
    hash = prime * hash + scoreMode.hashCode();
    hash = prime * hash + parentsFilter.hashCode();
    return hash;
  }
}
