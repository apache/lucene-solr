package org.apache.lucene.search;

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
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Bits;

/**
 *  Convenient base class for building queries that only perform matching, but
 *  no scoring. The scorer produced by such queries always returns 0 as score.
 */
public abstract class Filter extends Query {

  /**
   * Creates a {@link DocIdSet} enumerating the documents that should be
   * permitted in search results. <b>NOTE:</b> null can be
   * returned if no documents are accepted by this Filter.
   * <p>
   * Note: This method will be called once per segment in
   * the index during searching.  The returned {@link DocIdSet}
   * must refer to document IDs for that segment, not for
   * the top-level reader.
   *
   * @param context a {@link org.apache.lucene.index.LeafReaderContext} instance opened on the index currently
   *         searched on. Note, it is likely that the provided reader info does not
   *         represent the whole underlying index i.e. if the index has more than
   *         one segment the given reader only represents a single segment.
   *         The provided context is always an atomic context, so you can call
   *         {@link org.apache.lucene.index.LeafReader#fields()}
   *         on the context's reader, for example.
   *
   * @param acceptDocs
   *          Bits that represent the allowable docs to match (typically deleted docs
   *          but possibly filtering other documents)
   *
   * @return a DocIdSet that provides the documents which should be permitted or
   *         prohibited in search results. <b>NOTE:</b> <code>null</code> should be returned if
   *         the filter doesn't accept any documents otherwise internal optimization might not apply
   *         in the case an <i>empty</i> {@link DocIdSet} is returned.
   */
  public abstract DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException;

  //
  // Query compatibility
  //

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new Weight(this) {

      @Override
      public void extractTerms(Set<Term> terms) {}

      @Override
      public float getValueForNormalization() throws IOException {
        return 0f;
      }

      @Override
      public void normalize(float norm, float topLevelBoost) {}

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        final Scorer scorer = scorer(context, context.reader().getLiveDocs());
        final boolean match = (scorer != null && scorer.advance(doc) == doc);
        if (match) {
          assert scorer.score() == 0f;
          return Explanation.match(0f, "Match on id " + doc);
        } else {
          return Explanation.match(0f, "No match on id " + doc);
        }
      }

      @Override
      public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
        final DocIdSet set = getDocIdSet(context, acceptDocs);
        if (set == null) {
          return null;
        }
        final DocIdSetIterator iterator = set.iterator();
        if (iterator == null) {
          return null;
        }
        return new Scorer(this) {

          @Override
          public float score() throws IOException {
            return 0f;
          }

          @Override
          public int freq() throws IOException {
            return 1;
          }

          @Override
          public int docID() {
            return iterator.docID();
          }

          @Override
          public int nextDoc() throws IOException {
            return iterator.nextDoc();
          }

          @Override
          public int advance(int target) throws IOException {
            return iterator.advance(target);
          }

          @Override
          public long cost() {
            return iterator.cost();
          }
        };
      }

    };
  }
}
