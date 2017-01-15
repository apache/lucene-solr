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

package org.apache.lucene.search.grouping.term;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.AllGroupHeadsCollector;
import org.apache.lucene.search.grouping.AllGroupsCollector;
import org.apache.lucene.search.grouping.FirstPassGroupingCollector;
import org.apache.lucene.search.grouping.SecondPassGroupingCollector;
import org.apache.lucene.search.grouping.Grouper;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.BytesRef;

/**
 * Collector factory for grouping by term
 */
public class TermGrouper extends Grouper<BytesRef> {

  private final String field;
  private final int initialSize;

  /**
   * Create a new TermGrouper
   * @param field the field to group on
   */
  public TermGrouper(String field) {
    this(field, 128);
  }

  /**
   * Create a new TermGrouper
   * @param field       the field to group on
   * @param initialSize the initial size of various internal datastructures
   */
  public TermGrouper(String field, int initialSize) {
    this.field = field;
    this.initialSize = initialSize;
  }

  @Override
  public FirstPassGroupingCollector<BytesRef> getFirstPassCollector(Sort sort, int count) throws IOException {
    return new TermFirstPassGroupingCollector(field, sort, count);
  }

  @Override
  public AllGroupHeadsCollector<BytesRef> getGroupHeadsCollector(Sort sort) {
    return TermAllGroupHeadsCollector.create(field, sort, initialSize);
  }

  @Override
  public AllGroupsCollector<BytesRef> getAllGroupsCollector() {
    return new TermAllGroupsCollector(field, initialSize);
  }

  @Override
  public SecondPassGroupingCollector<BytesRef> getSecondPassCollector(
      Collection<SearchGroup<BytesRef>> groups, Sort groupSort, Sort withinGroupSort,
      int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields) throws IOException {
    return new TermSecondPassGroupingCollector(field, groups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields);
  }


}
