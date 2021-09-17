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

package org.apache.lucene.facet;

import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;

/**
 * Utility class with a single method for getting a DocIdSetIterator that skips deleted docs
 *
 * @lucene.experimental
 */
public final class FacetUtils {

  /** Do not instantiate this class */
  private FacetUtils() {}

  /**
   * Wrap the given DocIdSetIterator and liveDocs into another DocIdSetIterator that returns
   * non-deleted documents during iteration. This is useful for computing facet counts on match-all
   * style queries that need to exclude deleted documents.
   *
   * <p>Making this class pkg-private unfortunately limits the visibility of this method to {@link
   * org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts} and {@link
   * org.apache.lucene.facet.sortedset.ConcurrentSortedSetDocValuesFacetCounts} classes as Java does
   * not allow pkg-private classes to be visible to sub-packages.
   *
   * @param it {@link DocIdSetIterator} being wrapped
   * @param liveDocs {@link Bits} containing set bits for non-deleted docs
   * @return wrapped iterator
   */
  public static DocIdSetIterator liveDocsDISI(DocIdSetIterator it, Bits liveDocs) {

    return new DocIdSetIterator() {
      @Override
      public int docID() {
        return it.docID();
      }

      private int doNext(int doc) throws IOException {
        assert doc == it.docID();
        // Find next document that is not deleted until we exhaust all documents
        while (doc != NO_MORE_DOCS && liveDocs.get(doc) == false) {
          doc = it.nextDoc();
        }
        return doc;
      }

      @Override
      public int nextDoc() throws IOException {
        return doNext(it.nextDoc());
      }

      @Override
      public int advance(int target) throws IOException {
        return doNext(it.advance(target));
      }

      @Override
      public long cost() {
        return it.cost();
      }
    };
  }
}
