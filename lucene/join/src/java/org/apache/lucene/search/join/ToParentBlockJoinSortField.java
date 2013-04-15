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

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.SortField;

import java.io.IOException;

/**
 * A special sort field that allows sorting parent docs based on nested / child level fields.
 * Based on the sort order it either takes the document with the lowest or highest field value into account.
 *
 * @lucene.experimental
 */
public class ToParentBlockJoinSortField extends SortField {

  private final boolean order;
  private final Filter parentFilter;
  private final Filter childFilter;

  /**
   * Create ToParentBlockJoinSortField. The parent document ordering is based on child document ordering (reverse).
   *
   * @param field The sort field on the nested / child level.
   * @param type The sort type on the nested / child level.
   * @param reverse Whether natural order should be reversed on the nested / child level.
   * @param parentFilter Filter that identifies the parent documents.
   * @param childFilter Filter that defines which child documents participates in sorting.
   */
  public ToParentBlockJoinSortField(String field, Type type, boolean reverse, Filter parentFilter, Filter childFilter) {
    super(field, type, reverse);
    this.order = reverse;
    this.parentFilter = parentFilter;
    this.childFilter = childFilter;
  }

  /**
   * Create ToParentBlockJoinSortField.
   *
   * @param field The sort field on the nested / child level.
   * @param type The sort type on the nested / child level.
   * @param reverse Whether natural order should be reversed on the nested / child document level.
   * @param order Whether natural order should be reversed on the parent level.
   * @param parentFilter Filter that identifies the parent documents.
   * @param childFilter Filter that defines which child documents participates in sorting.
   */
  public ToParentBlockJoinSortField(String field, Type type, boolean reverse, boolean order, Filter parentFilter, Filter childFilter) {
    super(field, type, reverse);
    this.order = order;
    this.parentFilter = parentFilter;
    this.childFilter = childFilter;
  }

  @Override
  @SuppressWarnings("unchecked")
  public FieldComparator<?> getComparator(int numHits, int sortPos) throws IOException {
    FieldComparator<Object> wrappedFieldComparator = (FieldComparator) super.getComparator(numHits + 1, sortPos);
    if (order) {
      return new ToParentBlockJoinFieldComparator.Highest(wrappedFieldComparator, parentFilter, childFilter, numHits);
    } else {
      return new ToParentBlockJoinFieldComparator.Lowest(wrappedFieldComparator, parentFilter, childFilter, numHits);
    }
  }

}
