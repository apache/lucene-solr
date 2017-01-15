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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSet;

/**
 * This query requires that you index
 * children and parent docs as a single block, using the
 * {@link IndexWriter#addDocuments IndexWriter.addDocuments()} or {@link
 * IndexWriter#updateDocuments IndexWriter.updateDocuments()} API.  In each block, the
 * child documents must appear first, ending with the parent
 * document.  At search time you provide a Filter
 * identifying the parents, however this Filter must provide
 * an {@link BitSet} per sub-reader.
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

  private final BitSetProducer parentsFilter;
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
   * @param parentsFilter Filter identifying the parent documents.
   * @param scoreMode How to aggregate multiple child scores
   * into a single parent score.
   **/
  public ToParentBlockJoinQuery(Query childQuery, BitSetProducer parentsFilter, ScoreMode scoreMode) {
    super();
    this.origChildQuery = childQuery;
    this.childQuery = childQuery;
    this.parentsFilter = parentsFilter;
    this.scoreMode = scoreMode;
  }

  private ToParentBlockJoinQuery(Query origChildQuery, Query childQuery, BitSetProducer parentsFilter, ScoreMode scoreMode) {
    super();
    this.origChildQuery = origChildQuery;
    this.childQuery = childQuery;
    this.parentsFilter = parentsFilter;
    this.scoreMode = scoreMode;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    return new BlockJoinWeight(this, childQuery.createWeight(searcher, needsScores, boost), parentsFilter, needsScores ? scoreMode : ScoreMode.None);
  }
  
  /** Return our child query. */
  public Query getChildQuery() {
    return childQuery;
  }

  private static class BlockJoinWeight extends FilterWeight {
    private final BitSetProducer parentsFilter;
    private final ScoreMode scoreMode;

    public BlockJoinWeight(Query joinQuery, Weight childWeight, BitSetProducer parentsFilter, ScoreMode scoreMode) {
      super(joinQuery, childWeight);
      this.parentsFilter = parentsFilter;
      this.scoreMode = scoreMode;
    }

    // NOTE: acceptDocs applies (and is checked) only in the
    // parent document space
    @Override
    public Scorer scorer(LeafReaderContext readerContext) throws IOException {

      final Scorer childScorer = in.scorer(readerContext);
      if (childScorer == null) {
        // No matches
        return null;
      }

      final int firstChildDoc = childScorer.iterator().nextDoc();
      if (firstChildDoc == DocIdSetIterator.NO_MORE_DOCS) {
        // No matches
        return null;
      }

      // NOTE: this does not take accept docs into account, the responsibility
      // to not match deleted docs is on the scorer
      final BitSet parents = parentsFilter.getBitSet(readerContext);

      if (parents == null) {
        // No matches
        return null;
      }

      return new BlockJoinScorer(this, childScorer, parents, firstChildDoc, scoreMode);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      BlockJoinScorer scorer = (BlockJoinScorer) scorer(context);
      if (scorer != null && scorer.iterator().advance(doc) == doc) {
        return scorer.explain(context, in);
      }
      return Explanation.noMatch("Not a match");
    }
  }
  
  /** 
   * Ascendant for {@link ToParentBlockJoinQuery}'s scorer. 
   * @lucene.experimental it might be removed at <b>6.0</b>
   * */
  public static abstract class ChildrenMatchesScorer extends Scorer{
    
    /** inherited constructor */
    protected ChildrenMatchesScorer(Weight weight) {
      super(weight);
    }
    
    /** 
     * enables children matches recording 
     * */
    public abstract void trackPendingChildHits() ;
    
    /**
     * reports matched children 
     * @return number of recorded matched children docs 
     * */
    public abstract int getChildCount() ;
    
    /**
     * reports matched children 
     * @param other array for recording matching children docs of next parent,
     * it might be null (that's slower) or the same array which was returned 
     * from the previous call
     * @return array with {@link #getChildCount()} matched children docnums
     *  */
    public abstract int[] swapChildDocs(int[] other);
  }
  
  static class BlockJoinScorer extends ChildrenMatchesScorer {
    private final Scorer childScorer;
    private final BitSet parentBits;
    private final ScoreMode scoreMode;
    private int parentDoc = -1;
    private int prevParentDoc;
    private float parentScore;
    private int parentFreq;
    private int nextChildDoc;
    private int[] pendingChildDocs;
    private float[] pendingChildScores;
    private int childDocUpto;

    public BlockJoinScorer(Weight weight, Scorer childScorer, BitSet parentBits, int firstChildDoc, ScoreMode scoreMode) {
      super(weight);
      //System.out.println("Q.init firstChildDoc=" + firstChildDoc);
      this.parentBits = parentBits;
      this.childScorer = childScorer;
      this.scoreMode = scoreMode;
      nextChildDoc = firstChildDoc;
    }

    @Override
    public Collection<ChildScorer> getChildren() {
      return Collections.singleton(new ChildScorer(childScorer, "BLOCK_JOIN"));
    }

    @Override
    public int getChildCount() {
      return childDocUpto;
    }

    int getParentDoc() {
      return parentDoc;
    }

    @Override
    public int[] swapChildDocs(int[] other) {
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
    public DocIdSetIterator iterator() {
      return new DocIdSetIterator() {
        final DocIdSetIterator childIt = childScorer.iterator();

        @Override
        public int nextDoc() throws IOException {
          //System.out.println("Q.nextDoc() nextChildDoc=" + nextChildDoc);
          if (nextChildDoc == NO_MORE_DOCS) {
            //System.out.println("  end");
            return parentDoc = NO_MORE_DOCS;
          }

          // Gather all children sharing the same parent as
          // nextChildDoc

          parentDoc = parentBits.nextSetBit(nextChildDoc);

          // Parent & child docs are supposed to be
          // orthogonal:
          checkOrthogonal(nextChildDoc, parentDoc);

          //System.out.println("  parentDoc=" + parentDoc);
          assert parentDoc != DocIdSetIterator.NO_MORE_DOCS;

          float totalScore = 0;
          float maxScore = Float.NEGATIVE_INFINITY;
          float minScore = Float.POSITIVE_INFINITY;

          childDocUpto = 0;
          parentFreq = 0;
          do {

            //System.out.println("  c=" + nextChildDoc);
            if (pendingChildDocs != null && pendingChildDocs.length == childDocUpto) {
              pendingChildDocs = ArrayUtil.grow(pendingChildDocs);
            }
            if (pendingChildScores != null && scoreMode != ScoreMode.None && pendingChildScores.length == childDocUpto) {
              pendingChildScores = ArrayUtil.grow(pendingChildScores);
            }
            if (pendingChildDocs != null) {
              pendingChildDocs[childDocUpto] = nextChildDoc;
            }
            if (scoreMode != ScoreMode.None) {
              // TODO: specialize this into dedicated classes per-scoreMode
              final float childScore = childScorer.score();
              final int childFreq = childScorer.freq();
              if (pendingChildScores != null) {
                pendingChildScores[childDocUpto] = childScore;
              }
              maxScore = Math.max(childScore, maxScore);
              minScore = Math.min(childScore, minScore);
              totalScore += childScore;
              parentFreq += childFreq;
            }
            childDocUpto++;
            nextChildDoc = childIt.nextDoc();
          } while (nextChildDoc < parentDoc);

          // Parent & child docs are supposed to be
          // orthogonal:
          checkOrthogonal(nextChildDoc, parentDoc);

          switch(scoreMode) {
          case Avg:
            parentScore = totalScore / childDocUpto;
            break;
          case Max:
            parentScore = maxScore;
            break;
          case Min:
            parentScore = minScore;
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
            nextChildDoc = childIt.advance(prevParentDoc);
            // System.out.println("  childScorer advanced to child docID=" + nextChildDoc);
          //} else {
            //System.out.println("  skip childScorer advance");
          }

          // Parent & child docs are supposed to be orthogonal:
          checkOrthogonal(nextChildDoc, prevParentDoc);

          final int nd = nextDoc();
          //System.out.println("  return nextParentDoc=" + nd);
          return nd;
        }

        @Override
        public int docID() {
          return parentDoc;
        }

        @Override
        public long cost() {
          return childIt.cost();
        }
      };
    }

    private void checkOrthogonal(int childDoc, int parentDoc) {
      if (childDoc==parentDoc) {
        throw new IllegalStateException("Child query must not match same docs with parent filter. "
             + "Combine them as must clauses (+) to find a problem doc. "
             + "docId=" + nextChildDoc + ", " + childScorer.getClass());
        
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

    public Explanation explain(LeafReaderContext context, Weight childWeight) throws IOException {
      int start = context.docBase + prevParentDoc + 1; // +1 b/c prevParentDoc is previous parent doc
      int end = context.docBase + parentDoc - 1; // -1 b/c parentDoc is parent doc

      Explanation bestChild = null;
      int matches = 0;
      for (int childDoc = start; childDoc <= end; childDoc++) {
        Explanation child = childWeight.explain(context, childDoc - context.docBase);
        if (child.isMatch()) {
          matches++;
          if (bestChild == null || child.getValue() > bestChild.getValue()) {
            bestChild = child;
          }
        }
      }

      return Explanation.match(score(), String.format(Locale.ROOT,
          "Score based on %d child docs in range from %d to %d, best match:", matches, start, end), bestChild
      );
    }

    /**
     * Instructs this scorer to keep track of the child docIds and score ids for retrieval purposes.
     */
    @Override
    public void trackPendingChildHits() {
      pendingChildDocs = new int[5];
      if (scoreMode != ScoreMode.None) {
        pendingChildScores = new float[5];
      }
    }
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final Query childRewrite = childQuery.rewrite(reader);
    if (childRewrite != childQuery) {
      return new ToParentBlockJoinQuery(origChildQuery,
                                childRewrite,
                                parentsFilter,
                                scoreMode);
    } else {
      return super.rewrite(reader);
    }
  }

  @Override
  public String toString(String field) {
    return "ToParentBlockJoinQuery ("+childQuery.toString()+")";
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(ToParentBlockJoinQuery other) {
    return origChildQuery.equals(other.origChildQuery) &&
           parentsFilter.equals(other.parentsFilter) &&
           scoreMode == other.scoreMode;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int hash = classHash();
    hash = prime * hash + origChildQuery.hashCode();
    hash = prime * hash + scoreMode.hashCode();
    hash = prime * hash + parentsFilter.hashCode();
    return hash;
  }
}
