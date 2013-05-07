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
import java.util.Set;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;       // javadocs
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Scorer.ChildScorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

/**
 * Just like {@link ToParentBlockJoinQuery}, except this
 * query joins in reverse: you provide a Query matching
 * parent documents and it joins down to child
 * documents.
 *
 * @lucene.experimental
 */

public class ToChildBlockJoinQuery extends Query {

  private final Filter parentsFilter;
  private final Query parentQuery;

  // If we are rewritten, this is the original parentQuery we
  // were passed; we use this for .equals() and
  // .hashCode().  This makes rewritten query equal the
  // original, so that user does not have to .rewrite() their
  // query before searching:
  private final Query origParentQuery;
  private final boolean doScores;

  /**
   * Create a ToChildBlockJoinQuery.
   * 
   * @param parentQuery Query that matches parent documents
   * @param parentsFilter Filter (must produce FixedBitSet
   * per-segment) identifying the parent documents.
   * @param doScores true if parent scores should be calculated
   */
  public ToChildBlockJoinQuery(Query parentQuery, Filter parentsFilter, boolean doScores) {
    super();
    this.origParentQuery = parentQuery;
    this.parentQuery = parentQuery;
    this.parentsFilter = parentsFilter;
    this.doScores = doScores;
  }

  private ToChildBlockJoinQuery(Query origParentQuery, Query parentQuery, Filter parentsFilter, boolean doScores) {
    super();
    this.origParentQuery = origParentQuery;
    this.parentQuery = parentQuery;
    this.parentsFilter = parentsFilter;
    this.doScores = doScores;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new ToChildBlockJoinWeight(this, parentQuery.createWeight(searcher), parentsFilter, doScores);
  }

  private static class ToChildBlockJoinWeight extends Weight {
    private final Query joinQuery;
    private final Weight parentWeight;
    private final Filter parentsFilter;
    private final boolean doScores;

    public ToChildBlockJoinWeight(Query joinQuery, Weight parentWeight, Filter parentsFilter, boolean doScores) {
      super();
      this.joinQuery = joinQuery;
      this.parentWeight = parentWeight;
      this.parentsFilter = parentsFilter;
      this.doScores = doScores;
    }

    @Override
    public Query getQuery() {
      return joinQuery;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return parentWeight.getValueForNormalization() * joinQuery.getBoost() * joinQuery.getBoost();
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      parentWeight.normalize(norm, topLevelBoost * joinQuery.getBoost());
    }

    // NOTE: acceptDocs applies (and is checked) only in the
    // child document space
    @Override
    public Scorer scorer(AtomicReaderContext readerContext, boolean scoreDocsInOrder,
        boolean topScorer, Bits acceptDocs) throws IOException {

      // Pass scoreDocsInOrder true, topScorer false to our sub:
      final Scorer parentScorer = parentWeight.scorer(readerContext, true, false, null);

      if (parentScorer == null) {
        // No matches
        return null;
      }

      // NOTE: we cannot pass acceptDocs here because this
      // will (most likely, justifiably) cause the filter to
      // not return a FixedBitSet but rather a
      // BitsFilteredDocIdSet.  Instead, we filter by
      // acceptDocs when we score:
      final DocIdSet parents = parentsFilter.getDocIdSet(readerContext, null);

      if (parents == null) {
        // No matches
        return null;
      }
      if (!(parents instanceof FixedBitSet)) {
        throw new IllegalStateException("parentFilter must return FixedBitSet; got " + parents);
      }

      return new ToChildBlockJoinScorer(this, parentScorer, (FixedBitSet) parents, doScores, acceptDocs);
    }

    @Override
    public Explanation explain(AtomicReaderContext reader, int doc) throws IOException {
      // TODO
      throw new UnsupportedOperationException(getClass().getName() +
                                              " cannot explain match on parent document");
    }

    @Override
    public boolean scoresDocsOutOfOrder() {
      return false;
    }
  }

  static class ToChildBlockJoinScorer extends Scorer {
    private final Scorer parentScorer;
    private final FixedBitSet parentBits;
    private final boolean doScores;
    private final Bits acceptDocs;

    private float parentScore;
    private int parentFreq = 1;

    private int childDoc = -1;
    private int parentDoc;

    public ToChildBlockJoinScorer(Weight weight, Scorer parentScorer, FixedBitSet parentBits, boolean doScores, Bits acceptDocs) {
      super(weight);
      this.doScores = doScores;
      this.parentBits = parentBits;
      this.parentScorer = parentScorer;
      this.acceptDocs = acceptDocs;
    }

    @Override
    public Collection<ChildScorer> getChildren() {
      return Collections.singleton(new ChildScorer(parentScorer, "BLOCK_JOIN"));
    }

    @Override
    public int nextDoc() throws IOException {
      //System.out.println("Q.nextDoc() parentDoc=" + parentDoc + " childDoc=" + childDoc);

      // Loop until we hit a childDoc that's accepted
      nextChildDoc:
      while (true) {
        if (childDoc+1 == parentDoc) {
          // OK, we are done iterating through all children
          // matching this one parent doc, so we now nextDoc()
          // the parent.  Use a while loop because we may have
          // to skip over some number of parents w/ no
          // children:
          while (true) {
            parentDoc = parentScorer.nextDoc();

            if (parentDoc == 0) {
              // Degenerate but allowed: parent has no children
              // TODO: would be nice to pull initial parent
              // into ctor so we can skip this if... but it's
              // tricky because scorer must return -1 for
              // .doc() on init...
              parentDoc = parentScorer.nextDoc();
            }

            if (parentDoc == NO_MORE_DOCS) {
              childDoc = NO_MORE_DOCS;
              //System.out.println("  END");
              return childDoc;
            }

            childDoc = 1 + parentBits.prevSetBit(parentDoc-1);

            if (acceptDocs != null && !acceptDocs.get(childDoc)) {
              continue nextChildDoc;
            }

            if (childDoc < parentDoc) {
              if (doScores) {
                parentScore = parentScorer.score();
                parentFreq = parentScorer.freq();
              }
              //System.out.println("  " + childDoc);
              return childDoc;
            } else {
              // Degenerate but allowed: parent has no children
            }
          }
        } else {
          assert childDoc < parentDoc: "childDoc=" + childDoc + " parentDoc=" + parentDoc;
          childDoc++;
          if (acceptDocs != null && !acceptDocs.get(childDoc)) {
            continue;
          }
          //System.out.println("  " + childDoc);
          return childDoc;
        }
      }
    }

    @Override
    public int docID() {
      return childDoc;
    }

    @Override
    public float score() throws IOException {
      return parentScore;
    }

    @Override
    public int freq() throws IOException {
      return parentFreq;
    }

    @Override
    public int advance(int childTarget) throws IOException {
      assert childTarget >= parentBits.length() || !parentBits.get(childTarget);
      
      //System.out.println("Q.advance childTarget=" + childTarget);
      if (childTarget == NO_MORE_DOCS) {
        //System.out.println("  END");
        return childDoc = parentDoc = NO_MORE_DOCS;
      }

      assert childDoc == -1 || childTarget != parentDoc: "childTarget=" + childTarget;
      if (childDoc == -1 || childTarget > parentDoc) {
        // Advance to new parent:
        parentDoc = parentScorer.advance(childTarget);
        //System.out.println("  advance to parentDoc=" + parentDoc);
        assert parentDoc > childTarget;
        if (parentDoc == NO_MORE_DOCS) {
          //System.out.println("  END");
          return childDoc = NO_MORE_DOCS;
        }
        if (doScores) {
          parentScore = parentScorer.score();
          parentFreq = parentScorer.freq();
        }
        final int firstChild = parentBits.prevSetBit(parentDoc-1);
        //System.out.println("  firstChild=" + firstChild);
        childTarget = Math.max(childTarget, firstChild);
      }

      assert childTarget < parentDoc;

      // Advance within children of current parent:
      childDoc = childTarget;
      //System.out.println("  " + childDoc);
      if (acceptDocs != null && !acceptDocs.get(childDoc)) {
        nextDoc();
      }
      return childDoc;
    }

    @Override
    public long cost() {
      return parentScorer.cost();
    }
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    parentQuery.extractTerms(terms);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final Query parentRewrite = parentQuery.rewrite(reader);
    if (parentRewrite != parentQuery) {
      Query rewritten = new ToChildBlockJoinQuery(parentQuery,
                                parentRewrite,
                                parentsFilter,
                                doScores);
      rewritten.setBoost(getBoost());
      return rewritten;
    } else {
      return this;
    }
  }

  @Override
  public String toString(String field) {
    return "ToChildBlockJoinQuery ("+parentQuery.toString()+")";
  }

  @Override
  public boolean equals(Object _other) {
    if (_other instanceof ToChildBlockJoinQuery) {
      final ToChildBlockJoinQuery other = (ToChildBlockJoinQuery) _other;
      return origParentQuery.equals(other.origParentQuery) &&
        parentsFilter.equals(other.parentsFilter) &&
        doScores == other.doScores &&
        super.equals(other);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int hash = super.hashCode();
    hash = prime * hash + origParentQuery.hashCode();
    hash = prime * hash + new Boolean(doScores).hashCode();
    hash = prime * hash + parentsFilter.hashCode();
    return hash;
  }

  @Override
  public ToChildBlockJoinQuery clone() {
    return new ToChildBlockJoinQuery(origParentQuery.clone(),
                                     parentsFilter,
                                     doScores);
  }
}
