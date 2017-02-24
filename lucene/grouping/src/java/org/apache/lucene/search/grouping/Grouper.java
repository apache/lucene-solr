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

package org.apache.lucene.search.grouping;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.search.Sort;

/**
 * A factory object to create first and second-pass collectors, run by a {@link GroupingSearch}
 * @param <T> the type the group value
 */
public abstract class Grouper<T> {

  /**
   * Create a first-pass collector
   * @param sort  the order in which groups should be returned
   * @param count how many groups to return
   */
  public abstract FirstPassGroupingCollector<T> getFirstPassCollector(Sort sort, int count) throws IOException;

  /**
   * Create an {@link AllGroupsCollector}
   */
  public abstract AllGroupsCollector<T> getAllGroupsCollector();

  /**
   * Create an {@link AllGroupHeadsCollector}
   * @param sort a within-group sort order to determine which doc is the group head
   */
  public abstract AllGroupHeadsCollector<T> getGroupHeadsCollector(Sort sort);

  /**
   * Create a second-pass collector
   */
  public abstract SecondPassGroupingCollector<T> getSecondPassCollector(
      Collection<SearchGroup<T>> groups, Sort groupSort, Sort withinGroupSort,
      int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields) throws IOException;

}
