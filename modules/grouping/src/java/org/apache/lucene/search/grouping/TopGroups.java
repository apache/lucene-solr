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

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;

/** Represents result returned by a grouping search.
 *
 * @lucene.experimental */
public class TopGroups<GROUP_VALUE_TYPE> {
  /** Number of documents matching the search */
  public final int totalHitCount;

  /** Number of documents grouped into the topN groups */
  public final int totalGroupedHitCount;

  /** The total number of unique groups. If <code>null</code> this value is not computed. */
  public final Integer totalGroupCount;

  /** Group results in groupSort order */
  public final GroupDocs<GROUP_VALUE_TYPE>[] groups;

  /** How groups are sorted against each other */
  public final SortField[] groupSort;

  /** How docs are sorted within each group */
  public final SortField[] withinGroupSort;

  public TopGroups(SortField[] groupSort, SortField[] withinGroupSort, int totalHitCount, int totalGroupedHitCount, GroupDocs<GROUP_VALUE_TYPE>[] groups) {
    this.groupSort = groupSort;
    this.withinGroupSort = withinGroupSort;
    this.totalHitCount = totalHitCount;
    this.totalGroupedHitCount = totalGroupedHitCount;
    this.groups = groups;
    this.totalGroupCount = null;
  }

  public TopGroups(TopGroups<GROUP_VALUE_TYPE> oldTopGroups, Integer totalGroupCount) {
    this.groupSort = oldTopGroups.groupSort;
    this.withinGroupSort = oldTopGroups.withinGroupSort;
    this.totalHitCount = oldTopGroups.totalHitCount;
    this.totalGroupedHitCount = oldTopGroups.totalGroupedHitCount;
    this.groups = oldTopGroups.groups;
    this.totalGroupCount = totalGroupCount;
  }

  /** Merges an array of TopGroups, for example obtained
   *  from the second-pass collector across multiple
   *  shards.  Each TopGroups must have been sorted by the
   *  same groupSort and docSort, and the top groups passed
   *  to all second-pass collectors must be the same.
   *
   * <b>NOTE</b>: We can't always compute an exact totalGroupCount.
   * Documents belonging to a group may occur on more than
   * one shard and thus the merged totalGroupCount can be
   * higher than the actual totalGroupCount. In this case the
   * totalGroupCount represents a upper bound. If the documents
   * of one group do only reside in one shard then the
   * totalGroupCount is exact.
   *
   * <b>NOTE</b>: the topDocs in each GroupDocs is actually
   * an instance of TopDocsAndShards
   */
  public static <T> TopGroups<T> merge(TopGroups<T>[] shardGroups, Sort groupSort, Sort docSort, int docOffset, int docTopN)
    throws IOException {

    //System.out.println("TopGroups.merge");

    if (shardGroups.length == 0) {
      return null;
    }

    int totalHitCount = 0;
    int totalGroupedHitCount = 0;
    // Optionally merge the totalGroupCount.
    Integer totalGroupCount = null;

    final int numGroups = shardGroups[0].groups.length;
    for(TopGroups<T> shard : shardGroups) {
      if (numGroups != shard.groups.length) {
        throw new IllegalArgumentException("number of groups differs across shards; you must pass same top groups to all shards' second-pass collector");
      }
      totalHitCount += shard.totalHitCount;
      totalGroupedHitCount += shard.totalGroupedHitCount;
      if (shard.totalGroupCount != null) {
        if (totalGroupCount == null) {
          totalGroupCount = 0;
        }

        totalGroupCount += shard.totalGroupCount;
      }
    }

    @SuppressWarnings({"unchecked","rawtypes"})
    final GroupDocs<T>[] mergedGroupDocs = new GroupDocs[numGroups];

    final TopDocs[] shardTopDocs = new TopDocs[shardGroups.length];

    for(int groupIDX=0;groupIDX<numGroups;groupIDX++) {
      final T groupValue = shardGroups[0].groups[groupIDX].groupValue;
      //System.out.println("  merge groupValue=" + groupValue + " sortValues=" + Arrays.toString(shardGroups[0].groups[groupIDX].groupSortValues));
      float maxScore = Float.MIN_VALUE;
      int totalHits = 0;
      for(int shardIDX=0;shardIDX<shardGroups.length;shardIDX++) {
        //System.out.println("    shard=" + shardIDX);
        final TopGroups<T> shard = shardGroups[shardIDX];
        final GroupDocs<?> shardGroupDocs = shard.groups[groupIDX];
        if (groupValue == null) {
          if (shardGroupDocs.groupValue != null) {
            throw new IllegalArgumentException("group values differ across shards; you must pass same top groups to all shards' second-pass collector");
          }
        } else if (!groupValue.equals(shardGroupDocs.groupValue)) {
          throw new IllegalArgumentException("group values differ across shards; you must pass same top groups to all shards' second-pass collector");
        }

        /*
        for(ScoreDoc sd : shardGroupDocs.scoreDocs) {
          System.out.println("      doc=" + sd.doc);
        }
        */

        shardTopDocs[shardIDX] = new TopDocs(shardGroupDocs.totalHits,
                                             shardGroupDocs.scoreDocs,
                                             shardGroupDocs.maxScore);
        maxScore = Math.max(maxScore, shardGroupDocs.maxScore);
        totalHits += shardGroupDocs.totalHits;
      }

      final TopDocs mergedTopDocs = TopDocs.merge(docSort, docOffset + docTopN, shardTopDocs);

      // Slice;
      final ScoreDoc[] mergedScoreDocs;
      if (docOffset == 0) {
        mergedScoreDocs = mergedTopDocs.scoreDocs;
      } else if (docOffset >= mergedTopDocs.scoreDocs.length) {
        mergedScoreDocs = new ScoreDoc[0];
      } else {
        mergedScoreDocs = new ScoreDoc[mergedTopDocs.scoreDocs.length - docOffset];
        System.arraycopy(mergedTopDocs.scoreDocs,
                         docOffset,
                         mergedScoreDocs,
                         0,
                         mergedTopDocs.scoreDocs.length - docOffset);
      }
      //System.out.println("SHARDS=" + Arrays.toString(mergedTopDocs.shardIndex));
      mergedGroupDocs[groupIDX] = new GroupDocs<T>(maxScore,
                                                   totalHits,
                                                   mergedScoreDocs,
                                                   groupValue,
                                                   shardGroups[0].groups[groupIDX].groupSortValues);
    }

    if (totalGroupCount != null) {
      TopGroups<T> result = new TopGroups<T>(groupSort.getSort(),
                              docSort == null ? null : docSort.getSort(),
                              totalHitCount,
                              totalGroupedHitCount,
                              mergedGroupDocs);
      return new TopGroups<T>(result, totalGroupCount);
    } else {
      return new TopGroups<T>(groupSort.getSort(),
                              docSort == null ? null : docSort.getSort(),
                              totalHitCount,
                              totalGroupedHitCount,
                              mergedGroupDocs);
    }
  }
}
