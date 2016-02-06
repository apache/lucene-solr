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
package org.apache.lucene.spatial.prefix;

import java.io.IOException;

import com.spatial4j.core.shape.Shape;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.DocIdSetBuilder;

/**
 * Base class for Lucene Queries on SpatialPrefixTree fields.
 * @lucene.internal
 */
public abstract class AbstractPrefixTreeQuery extends Query {

  protected final Shape queryShape;
  protected final String fieldName;
  protected final SpatialPrefixTree grid;//not in equals/hashCode since it's implied for a specific field
  protected final int detailLevel;

  public AbstractPrefixTreeQuery(Shape queryShape, String fieldName, SpatialPrefixTree grid, int detailLevel) {
    this.queryShape = queryShape;
    this.fieldName = fieldName;
    this.grid = grid;
    this.detailLevel = detailLevel;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (super.equals(o) == false) return false;

    AbstractPrefixTreeQuery that = (AbstractPrefixTreeQuery) o;

    if (detailLevel != that.detailLevel) return false;
    if (!fieldName.equals(that.fieldName)) return false;
    if (!queryShape.equals(that.queryShape)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + queryShape.hashCode();
    result = 31 * result + fieldName.hashCode();
    result = 31 * result + detailLevel;
    return result;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new ConstantScoreWeight(this) {
      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        DocIdSet docSet = getDocIdSet(context);
        if (docSet == null) {
          return null;
        }
        DocIdSetIterator disi = docSet.iterator();
        if (disi == null) {
          return null;
        }
        return new ConstantScoreScorer(this, score(), disi);
      }
    };
  }

  protected abstract DocIdSet getDocIdSet(LeafReaderContext context) throws IOException;

  /** Holds transient state and docid collecting utility methods as part of
   * traversing a {@link TermsEnum} for a {@link org.apache.lucene.index.LeafReaderContext}. */
  public abstract class BaseTermsEnumTraverser {//TODO rename to LeafTermsEnumTraverser ?
    //note: only 'fieldName' (accessed in constructor) keeps this from being a static inner class

    protected final LeafReaderContext context;
    protected final int maxDoc;

    protected TermsEnum termsEnum;//remember to check for null!
    protected PostingsEnum postingsEnum;

    public BaseTermsEnumTraverser(LeafReaderContext context) throws IOException {
      this.context = context;
      LeafReader reader = context.reader();
      this.maxDoc = reader.maxDoc();
      Terms terms = reader.terms(fieldName);
      if (terms != null)
        this.termsEnum = terms.iterator();
    }

    protected void collectDocs(BitSet bitSet) throws IOException {
      assert termsEnum != null;
      postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
      bitSet.or(postingsEnum);
    }

    protected void collectDocs(DocIdSetBuilder docSetBuilder) throws IOException {
      assert termsEnum != null;
      postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
      docSetBuilder.add(postingsEnum);
    }
  }

}
