package org.apache.lucene.search.grouping;

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

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Collection;

/**
 * A collector that collects all groups that match the
 * query. Only the group value is collected, and the order
 * is undefined.  This collector does not determine
 * the most relevant document of a group.
 *
 * <p/>
 * This is an abstract version. Concrete implementations define
 * what a group actually is and how it is internally collected.
 *
 * @lucene.experimental
 */
public abstract class AbstractAllGroupsCollector<GROUP_VALUE_TYPE> extends Collector {

  /**
   * Returns the total number of groups for the executed search.
   * This is a convenience method. The following code snippet has the same effect: <pre>getGroups().size()</pre>
   *
   * @return The total number of groups for the executed search
   */
  public int getGroupCount() {
    return getGroups().size();
  }

  /**
   * Returns the group values
   * <p/>
   * This is an unordered collections of group values. For each group that matched the query there is a {@link BytesRef}
   * representing a group value.
   *
   * @return the group values
   */
  public abstract Collection<GROUP_VALUE_TYPE> getGroups();

  // Empty not necessary
  @Override
  public void setScorer(Scorer scorer) throws IOException {}

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return true;
  }
}