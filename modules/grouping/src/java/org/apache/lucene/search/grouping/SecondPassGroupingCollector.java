package org.apache.lucene.search.grouping;

/**
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
import java.util.Collection;
import java.util.HashMap;

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.util.BytesRef;

/**
 * SecondPassGroupingCollector is the second of two passes
 * necessary to collect grouped docs.  This pass gathers the
 * top N documents per top group computed from the
 * first pass.
 *
 * <p>See {@link org.apache.lucene.search.grouping} for more
 * details including a full code example.</p>
 *
 * @lucene.experimental
 */
public class SecondPassGroupingCollector extends Collector {
  private final HashMap<BytesRef, SearchGroupDocs> groupMap;

  private FieldCache.DocTermsIndex index;
  private final String groupField;
  private final int maxDocsPerGroup;
  private final SentinelIntSet ordSet;
  private final SearchGroupDocs[] groupDocs;
  private final BytesRef spareBytesRef = new BytesRef();
  private final Collection<SearchGroup> groups;
  private final Sort withinGroupSort;
  private final Sort groupSort;

  private int totalHitCount;
  private int totalGroupedHitCount;

  public SecondPassGroupingCollector(String groupField, Collection<SearchGroup> groups, Sort groupSort, Sort withinGroupSort,
                                     int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields)
    throws IOException {

    //System.out.println("SP init");
    if (groups.size() == 0) {
      throw new IllegalArgumentException("no groups to collect (groups.size() is 0)");
    }

    this.groupSort = groupSort;
    this.withinGroupSort = withinGroupSort;
    this.groups = groups;
    this.groupField = groupField;
    this.maxDocsPerGroup = maxDocsPerGroup;

    groupMap = new HashMap<BytesRef, SearchGroupDocs>(groups.size());

    for (SearchGroup group : groups) {
      //System.out.println("  prep group=" + (group.groupValue == null ? "null" : group.groupValue.utf8ToString()));
      final TopDocsCollector collector;
      if (withinGroupSort == null) {
        // Sort by score
        collector = TopScoreDocCollector.create(maxDocsPerGroup, true);
      } else {
        // Sort by fields
        collector = TopFieldCollector.create(withinGroupSort, maxDocsPerGroup, fillSortFields, getScores, getMaxScores, true);
      }
      groupMap.put(group.groupValue,
                   new SearchGroupDocs(group.groupValue,
                                       collector));
    }

    ordSet = new SentinelIntSet(groupMap.size(), -1);
    groupDocs = new SearchGroupDocs[ordSet.keys.length];
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    for (SearchGroupDocs group : groupMap.values()) {
      group.collector.setScorer(scorer);
    }
  }

  @Override
  public void collect(int doc) throws IOException {
    final int slot = ordSet.find(index.getOrd(doc));
    //System.out.println("SP.collect doc=" + doc + " slot=" + slot);
    totalHitCount++;
    if (slot >= 0) {
      totalGroupedHitCount++;
      groupDocs[slot].collector.collect(doc);
    }
  }

  @Override
  public void setNextReader(AtomicReaderContext readerContext) throws IOException {
    //System.out.println("SP.setNextReader");
    for (SearchGroupDocs group : groupMap.values()) {
      group.collector.setNextReader(readerContext);
    }
    index = FieldCache.DEFAULT.getTermsIndex(readerContext.reader, groupField);

    // Rebuild ordSet
    ordSet.clear();
    for (SearchGroupDocs group : groupMap.values()) {
      //System.out.println("  group=" + (group.groupValue == null ? "null" : group.groupValue.utf8ToString()));
      int ord = group.groupValue == null ? 0 : index.binarySearchLookup(group.groupValue, spareBytesRef);
      if (ord >= 0) {
        groupDocs[ordSet.put(ord)] = group;
      }
    }
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }

  public TopGroups getTopGroups(int withinGroupOffset) {
    final GroupDocs[] groupDocsResult = new GroupDocs[groups.size()];

    int groupIDX = 0;
    for(SearchGroup group : groups) {
      final SearchGroupDocs groupDocs = groupMap.get(group.groupValue);
      final TopDocs topDocs = groupDocs.collector.topDocs(withinGroupOffset, maxDocsPerGroup);
      groupDocsResult[groupIDX++] = new GroupDocs(topDocs.getMaxScore(),
                                                  topDocs.totalHits,
                                                  topDocs.scoreDocs,
                                                  groupDocs.groupValue,
                                                  group.sortValues);
    }

    return new TopGroups(groupSort.getSort(),
                         withinGroupSort == null ? null : withinGroupSort.getSort(),
                         totalHitCount, totalGroupedHitCount, groupDocsResult);
  }
}


// TODO: merge with SearchGroup or not?
// ad: don't need to build a new hashmap
// disad: blows up the size of SearchGroup if we need many of them, and couples implementations
class SearchGroupDocs {
  public final BytesRef groupValue;
  public final TopDocsCollector collector;

  public SearchGroupDocs(BytesRef groupValue, TopDocsCollector collector) {
    this.groupValue = groupValue;
    this.collector = collector;
  }
}
