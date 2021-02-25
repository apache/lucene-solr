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
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;

/**
 * A collector that collects all groups that match the query. Only the group value is collected, and
 * the order is undefined. This collector does not determine the most relevant document of a group.
 *
 * @lucene.experimental
 */
public class AllGroupsCollector<T> extends SimpleCollector {

  private final GroupSelector<T> groupSelector;

  private final Set<T> groups = new HashSet<T>();

  /**
   * Create a new AllGroupsCollector
   *
   * @param groupSelector the GroupSelector to determine groups
   */
  public AllGroupsCollector(GroupSelector<T> groupSelector) {
    this.groupSelector = groupSelector;
  }

  /**
   * Returns the total number of groups for the executed search. This is a convenience method. The
   * following code snippet has the same effect:
   *
   * <pre>getGroups().size()</pre>
   *
   * @return The total number of groups for the executed search
   */
  public int getGroupCount() {
    return getGroups().size();
  }

  /**
   * Returns the group values
   *
   * <p>This is an unordered collections of group values.
   *
   * @return the group values
   */
  public Collection<T> getGroups() {
    return groups;
  }

  @Override
  public void setScorer(Scorable scorer) throws IOException {}

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    groupSelector.setNextReader(context);
  }

  @Override
  public void collect(int doc) throws IOException {
    groupSelector.advanceTo(doc);
    if (groups.contains(groupSelector.currentValue())) {
      return;
    }
    groups.add(groupSelector.copyValue());
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES; // the result is unaffected by relevancy
  }
}
