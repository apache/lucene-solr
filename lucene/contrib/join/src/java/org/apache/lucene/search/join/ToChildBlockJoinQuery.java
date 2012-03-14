package org.apache.lucene.search.join;

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
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;       // javadocs
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.CachingWrapperFilter; // javadocs
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.FixedBitSet;

/**
 * Just like {@link ToParentBlockJoinQuery}, except this
 * query joins in reverse: you provide a Query matching
 * parent documents and it joins down to child
 * documents.
 *
 * <p><b>WARNING</b>: to create the parents filter, always use
 * {@link RawTermFilter} (so that the filter
 * includes deleted docs), wrapped with {@link
 * CachingWrapperFilter} (so that the returned bit set per
 * reader is a {@link FixedBitSet}), specifying
 * DeletesMode.IGNORE (so that on reopen, the filter still
 * includes deleted docs).  Failure to do this can result in
 * completely wrong documents being returned!  For example:
 *
 * <pre>
 *   Filter parents = new CachingWrapperFilter(
 *                          new RawTermFilter(new Term("parent", "yes")),
 *                          CachingWrapperFilter.DeletesMode.IGNORE);
 * </pre>
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
  public Weight createWeight(Searcher searcher) throws IOException {
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
    public float getValue() {
      return parentWeight.getValue();
    }

    @Override
    public float sumOfSquaredWeights() throws IOException {
      return parentWeight.sumOfSquaredWeights() * joinQuery.getBoost() * joinQuery.getBoost();
    }

    @Override
    public void normalize(float norm) {
      parentWeight.normalize(norm * joinQuery.getBoost());
    }

    @Override
    public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) throws IOException {
      // Pass scoreDocsInOrder true, topScorer false to our sub:
      final Scorer parentScorer = parentWeight.scorer(reader, true, false);

      if (parentScorer == null) {
        // No matches
        return null;
      }

      final DocIdSet parents = parentsFilter.getDocIdSet(reader);
      // TODO: once we do random-access filters we can
      // generalize this:
      if (parents == null) {
        // No matches
        return null;
      }
      if (!(parents instanceof FixedBitSet)) {
        throw new IllegalStateException("parentFilter must return FixedBitSet; got " + parents);
      }

      return new ToChildBlockJoinScorer(this, parentScorer, (FixedBitSet) parents, doScores);
    }

    @Override
    public Explanation explain(IndexReader reader, int doc) throws IOException {
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
    private float parentScore;

    private int childDoc = -1;
    private int parentDoc;

    public ToChildBlockJoinScorer(Weight weight, Scorer parentScorer, FixedBitSet parentBits, boolean doScores) {
      super(weight);
      this.doScores = doScores;
      this.parentBits = parentBits;
      this.parentScorer = parentScorer;
    }

    @Override
    public void visitSubScorers(Query parent, BooleanClause.Occur relationship,
                                ScorerVisitor<Query, Query, Scorer> visitor) {
      super.visitSubScorers(parent, relationship, visitor);
      //parentScorer.visitSubScorers(weight.getQuery(), BooleanClause.Occur.MUST, visitor);
      parentScorer.visitScorers(visitor);
    }

    @Override
    public int nextDoc() throws IOException {
      //System.out.println("Q.nextDoc() parentDoc=" + parentDoc + " childDoc=" + childDoc);

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
          if (childDoc < parentDoc) {
            if (doScores) {
              parentScore = parentScorer.score();
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
        //System.out.println("  " + childDoc);
        return childDoc;
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
        }
        final int firstChild = parentBits.prevSetBit(parentDoc-1);
        //System.out.println("  firstChild=" + firstChild);
        childTarget = Math.max(childTarget, firstChild);
      }

      assert childTarget < parentDoc;

      // Advance within children of current parent:
      childDoc = childTarget;
      //System.out.println("  " + childDoc);
      return childDoc;
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
        doScores == other.doScores;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int hash = 1;
    hash = prime * hash + origParentQuery.hashCode();
    hash = prime * hash + new Boolean(doScores).hashCode();
    hash = prime * hash + parentsFilter.hashCode();
    return hash;
  }

  @Override
  public Object clone() {
    return new ToChildBlockJoinQuery((Query) origParentQuery.clone(),
                                     parentsFilter,
                                     doScores);
  }
}
