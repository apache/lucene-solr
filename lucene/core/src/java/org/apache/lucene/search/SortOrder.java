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

import org.apache.lucene.index.IndexSorter;
import org.apache.lucene.index.SortFieldProvider;

/**
 * Defines an ordering for documents within an index
 */
public interface SortOrder {

  /**
   * Returns whether the sort should be reversed.
   */
  boolean getReverse();

  /**
   * Returns the {@link FieldComparator} to use for sorting.
   *
   * @param numHits   number of top hits the queue will store
   * @param sortPos   position of this SortField within {@link Sort}.  The comparator is primary
   *                  if sortPos==0, secondary if sortPos==1, etc.  Some comparators can
   *                  optimize themselves when they are the primary sort.
   */
  FieldComparator<?> getComparator(int numHits, int sortPos);

  /**
   * Whether the relevance score is needed to sort documents.
   */
  boolean needsScores();

  /**
   * A name for the sort order
   */
  default String name() {
    return toString();
  }

  /**
   * Rewrites this SortOrder, returning a new SortOrder if a change is made.
   *
   * @param searcher IndexSearcher to use during rewriting
   * @return New rewritten SortOrder, or {@code this} if nothing has changed.
   */
  default SortOrder rewrite(IndexSearcher searcher) throws IOException {
    return this;
  }

  /**
   * Returns an {@link IndexSorter} used for sorting index segments by this SortField.
   *
   * If the SortField cannot be used for index sorting (for example, if it uses scores or
   * other query-dependent values) then this method should return {@code null}
   *
   * SortFields that implement this method should also implement a companion
   * {@link SortFieldProvider} to serialize and deserialize the sort in index segment
   * headers
   *
   * @lucene.experimental
   */
  IndexSorter getIndexSorter();

  SortOrder SCORE = new SortOrder() {
    @Override
    public boolean getReverse() {
      return false;
    }

    @Override
    public FieldComparator<?> getComparator(int numHits, int sortPos) {
      return new FieldComparator.RelevanceComparator(numHits);
    }

    @Override
    public boolean needsScores() {
      return true;
    }

    @Override
    public IndexSorter getIndexSorter() {
      return null;
    }

    @Override
    public String toString() {
      return "<score>";
    }
  };

  SortOrder DOC_ID = new SortOrder() {
    @Override
    public boolean getReverse() {
      return false;
    }

    @Override
    public FieldComparator<?> getComparator(int numHits, int sortPos) {
      return new FieldComparator.DocComparator(numHits);
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public IndexSorter getIndexSorter() {
      return null;
    }

    @Override
    public String toString() {
      return "<docid>";
    }
  };

}
