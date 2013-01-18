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

import java.io.IOException;
import java.util.*;

/**
 * A second pass grouping collector that keeps track of distinct values for a specified field for the top N group.
 *
 * @lucene.experimental
 */
public abstract class AbstractDistinctValuesCollector<GC extends AbstractDistinctValuesCollector.GroupCount<?>> extends Collector {

  /**
   * Returns all unique values for each top N group.
   *
   * @return all unique values for each top N group
   */
  public abstract List<GC> getGroups();

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return true;
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
  }

  /**
   * Returned by {@link AbstractDistinctValuesCollector#getGroups()},
   * representing the value and set of distinct values for the group.
   */
  public abstract static class GroupCount<GROUP_VALUE_TYPE> {

    public final GROUP_VALUE_TYPE groupValue;
    public final Set<GROUP_VALUE_TYPE> uniqueValues;

    public GroupCount(GROUP_VALUE_TYPE groupValue) {
      this.groupValue = groupValue;
      this.uniqueValues = new HashSet<GROUP_VALUE_TYPE>();
    }
  }

}
