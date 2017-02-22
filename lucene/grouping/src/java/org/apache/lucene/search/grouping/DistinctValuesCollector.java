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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.search.SimpleCollector;

/**
 * A second pass grouping collector that keeps track of distinct values for a specified field for the top N group.
 *
 * @lucene.experimental
 */
public abstract class DistinctValuesCollector<T> extends SimpleCollector {

  /**
   * Returns all unique values for each top N group.
   *
   * @return all unique values for each top N group
   */
  public abstract List<GroupCount<T>> getGroups();

  /**
   * Returned by {@link DistinctValuesCollector#getGroups()},
   * representing the value and set of distinct values for the group.
   */
  public static class GroupCount<T> {

    public final T groupValue;
    public final Set<T> uniqueValues;

    public GroupCount(T groupValue) {
      this.groupValue = groupValue;
      this.uniqueValues = new HashSet<>();
    }
  }

  @Override
  public boolean needsScores() {
    return false; // not needed to fetch all values
  }

}
