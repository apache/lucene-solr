package org.apache.lucene.search.grouping.term;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.grouping.AbstractAllGroupsCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SentinelIntSet;

/**
 * A collector that collects all groups that match the
 * query. Only the group value is collected, and the order
 * is undefined.  This collector does not determine
 * the most relevant document of a group.
 *
 * <p/>
 * Implementation detail: an int hash set (SentinelIntSet)
 * is used to detect if a group is already added to the
 * total count.  For each segment the int set is cleared and filled
 * with previous counted groups that occur in the new
 * segment.
 *
 * @lucene.experimental
 */
public class TermAllGroupsCollector extends AbstractAllGroupsCollector<BytesRef> {

  private static final int DEFAULT_INITIAL_SIZE = 128;

  private final String groupField;
  private final SentinelIntSet ordSet;
  private final List<BytesRef> groups;

  private SortedDocValues index;

  /**
   * Expert: Constructs a {@link AbstractAllGroupsCollector}
   *
   * @param groupField  The field to group by
   * @param initialSize The initial allocation size of the
   *                    internal int set and group list
   *                    which should roughly match the total
   *                    number of expected unique groups. Be aware that the
   *                    heap usage is 4 bytes * initialSize.
   */
  public TermAllGroupsCollector(String groupField, int initialSize) {
    ordSet = new SentinelIntSet(initialSize, -2);
    groups = new ArrayList<BytesRef>(initialSize);
    this.groupField = groupField;
  }

  /**
   * Constructs a {@link AbstractAllGroupsCollector}. This sets the
   * initial allocation size for the internal int set and group
   * list to 128.
   *
   * @param groupField The field to group by
   */
  public TermAllGroupsCollector(String groupField) {
    this(groupField, DEFAULT_INITIAL_SIZE);
  }

  @Override
  public void collect(int doc) throws IOException {
    int key = index.getOrd(doc);
    if (!ordSet.exists(key)) {
      ordSet.put(key);
      BytesRef term;
      if (key == -1) {
        term = null;
      } else {
        term =  new BytesRef();
        index.lookupOrd(key, term);
      }
      groups.add(term);
    }
  }

  @Override
  public Collection<BytesRef> getGroups() {
    return groups;
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    index = FieldCache.DEFAULT.getTermsIndex(context.reader(), groupField);

    // Clear ordSet and fill it with previous encountered groups that can occur in the current segment.
    ordSet.clear();
    for (BytesRef countedGroup : groups) {
      if (countedGroup == null) {
        ordSet.put(-1);
      } else {
        int ord = index.lookupTerm(countedGroup);
        if (ord >= 0) {
          ordSet.put(ord);
        }
      }
    }
  }
}
