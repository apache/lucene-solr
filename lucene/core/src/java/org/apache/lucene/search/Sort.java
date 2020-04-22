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
import java.util.Arrays;


/**
 * Encapsulates sort criteria for a set of documents
 */
public class Sort {

  /**
   * Represents sorting by computed relevance. Using this sort criteria returns
   * the same results as calling
   * {@link IndexSearcher#search(Query,int) IndexSearcher#search()}without a sort criteria,
   * only with slightly more overhead.
   */
  public static final Sort RELEVANCE = new Sort();

  /** Represents sorting by index order. */
  public static final Sort INDEXORDER = new Sort(SortField.FIELD_DOC);

  // internal representation of the sort criteria
  private final SortOrder[] fields;

  /**
   * Sorts by computed relevance. This is the same sort criteria as calling
   * {@link IndexSearcher#search(Query,int) IndexSearcher#search()}without a sort criteria,
   * only with slightly more overhead.
   */
  public Sort() {
    this(SortOrder.SCORE);
  }

  /** Sorts by the criteria in the given SortField. */
  public Sort(SortOrder field) {
    this.fields = new SortOrder[] { field };
  }

  /** Sets the sort to the given criteria in succession: the
   *  first SortOrder is checked first, but if it produces a
   *  tie, then the second SortOrder is used to break the tie,
   *  etc.  Finally, if there is still a tie after all SortOrders
   *  are checked, the internal Lucene docid is used to break it. */
  public Sort(SortOrder... fields) {
    if (fields.length == 0) {
      throw new IllegalArgumentException("There must be at least 1 sort field");
    }
    this.fields = fields;
  }
  
  /**
   * Representation of the sort criteria.
   * @return Array of SortField objects used in this sort criteria
   */
  public SortOrder[] getSort() {
    return fields;
  }

  /**
   * Rewrites the SortOrders in this Sort, returning a new Sort if any of them
   * change during their rewriting.
   *
   * @param searcher IndexSearcher to use in the rewriting
   * @return {@code this} if the SortOrders have not changed, or a new Sort if there
   *        is a change
   * @throws IOException Can be thrown by the rewriting
   */
  public Sort rewrite(IndexSearcher searcher) throws IOException {
    boolean changed = false;

    SortOrder[] rewrittenSortFields = new SortOrder[fields.length];
    for (int i = 0; i < fields.length; i++) {
      rewrittenSortFields[i] = fields[i].rewrite(searcher);
      if (fields[i] != rewrittenSortFields[i]) {
        changed = true;
      }
    }

    return (changed) ? new Sort(rewrittenSortFields) : this;
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();

    for (int i = 0; i < fields.length; i++) {
      buffer.append(fields[i].toString());
      if ((i+1) < fields.length)
        buffer.append(',');
    }

    return buffer.toString();
  }

  /** Returns true if <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Sort)) return false;
    final Sort other = (Sort)o;
    return Arrays.equals(this.fields, other.fields);
  }

  /** Returns a hash code value for this object. */
  @Override
  public int hashCode() {
    return 0x45aaf665 + Arrays.hashCode(fields);
  }

  /** Returns true if the relevance score is needed to sort documents. */
  public boolean needsScores() {
    for (SortOrder sortField : fields) {
      if (sortField.needsScores()) {
        return true;
      }
    }
    return false;
  }

}
