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

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;

/** Represents result returned by a grouping search.
 *
 * @lucene.experimental */
public class TopGroups<T> {
  /** Number of documents matching the search */
  public final int totalHitCount;

  /** Number of documents grouped into the topN groups */
  public final int totalGroupedHitCount;

  /** The total number of unique groups. If <code>null</code> this value is not computed. */
  public final Integer totalGroupCount;

  /** Group results in groupSort order */
  public final GroupDocs<T>[] groups;

  /** How groups are sorted against each other */
  public final SortField[] groupSort;

  /** How docs are sorted within each group */
  public final SortField[] withinGroupSort;

  /** Highest score across all hits, or
   *  <code>Float.NaN</code> if scores were not computed. */
  public final float maxScore;

  public TopGroups(SortField[] groupSort, SortField[] withinGroupSort, int totalHitCount, int totalGroupedHitCount, GroupDocs<T>[] groups, float maxScore) {
    this.groupSort = groupSort;
    this.withinGroupSort = withinGroupSort;
    this.totalHitCount = totalHitCount;
    this.totalGroupedHitCount = totalGroupedHitCount;
    this.groups = groups;
    this.totalGroupCount = null;
    this.maxScore = maxScore;
  }

  public TopGroups(TopGroups<T> oldTopGroups, Integer totalGroupCount) {
    this.groupSort = oldTopGroups.groupSort;
    this.withinGroupSort = oldTopGroups.withinGroupSort;
    this.totalHitCount = oldTopGroups.totalHitCount;
    this.totalGroupedHitCount = oldTopGroups.totalGroupedHitCount;
    this.groups = oldTopGroups.groups;
    this.maxScore = oldTopGroups.maxScore;
    this.totalGroupCount = totalGroupCount;
  }

  /** How the GroupDocs score (if any) should be merged. */
  public enum ScoreMergeMode {
    /** Set score to Float.NaN */
    None,     
    /* Sum score across all shards for this group. */
    Total,
    /* Avg score across all shards for this group. */
    Avg,
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
  public static <T> TopGroups<T> merge(TopGroups<T>[] shardGroups, Sort groupSort, Sort docSort, int docOffset, int docTopN, ScoreMergeMode scoreMergeMode) {

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

    final TopDocs[] shardTopDocs;
    if (docSort.equals(Sort.RELEVANCE)) {
      shardTopDocs = new TopDocs[shardGroups.length];
    } else {
      shardTopDocs = new TopFieldDocs[shardGroups.length];
    }
    float totalMaxScore = Float.MIN_VALUE;

    for(int groupIDX=0;groupIDX<numGroups;groupIDX++) {
      final T groupValue = shardGroups[0].groups[groupIDX].groupValue;
      //System.out.println("  merge groupValue=" + groupValue + " sortValues=" + Arrays.toString(shardGroups[0].groups[groupIDX].groupSortValues));
      float maxScore = Float.MIN_VALUE;
      int totalHits = 0;
      double scoreSum = 0.0;
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

        if (docSort.equals(Sort.RELEVANCE)) {
          shardTopDocs[shardIDX] = new TopDocs(shardGroupDocs.totalHits,
                                               shardGroupDocs.scoreDocs);
        } else {
          shardTopDocs[shardIDX] = new TopFieldDocs(shardGroupDocs.totalHits,
              shardGroupDocs.scoreDocs,
              docSort.getSort());
        }

        for (int i = 0; i < shardTopDocs[shardIDX].scoreDocs.length; i++) {
          shardTopDocs[shardIDX].scoreDocs[i].shardIndex = shardIDX;
        }

        maxScore = Math.max(maxScore, shardGroupDocs.maxScore);
        assert shardGroupDocs.totalHits.relation == Relation.EQUAL_TO;
        totalHits += shardGroupDocs.totalHits.value;
        scoreSum += shardGroupDocs.score;
      }

      final TopDocs mergedTopDocs;
      if (docSort.equals(Sort.RELEVANCE)) {
        mergedTopDocs = TopDocs.merge(docOffset + docTopN, shardTopDocs);
      } else {
        mergedTopDocs = TopDocs.merge(docSort, docOffset + docTopN, (TopFieldDocs[]) shardTopDocs);
      }

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

      final float groupScore;
      switch(scoreMergeMode) {
      case None:
        groupScore = Float.NaN;
        break;
      case Avg:
        if (totalHits > 0) {
          groupScore = (float) (scoreSum / totalHits);
        } else {
          groupScore = Float.NaN;
        }
        break;
      case Total:
        groupScore = (float) scoreSum;
        break;
      default:
        throw new IllegalArgumentException("can't handle ScoreMergeMode " + scoreMergeMode);
      }
        
      //System.out.println("SHARDS=" + Arrays.toString(mergedTopDocs.shardIndex));
      mergedGroupDocs[groupIDX] = new GroupDocs<>(groupScore,
                                                   maxScore,
                                                   new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO),
                                                   mergedScoreDocs,
                                                   groupValue,
                                                   shardGroups[0].groups[groupIDX].groupSortValues);
      totalMaxScore = Math.max(totalMaxScore, maxScore);
    }

    if (totalGroupCount != null) {
      TopGroups<T> result = new TopGroups<>(groupSort.getSort(),
                              docSort.getSort(),
                              totalHitCount,
                              totalGroupedHitCount,
                              mergedGroupDocs,
                              totalMaxScore);
      return new TopGroups<>(result, totalGroupCount);
    } else {
      return new TopGroups<>(groupSort.getSort(),
                              docSort.getSort(),
                              totalHitCount,
                              totalGroupedHitCount,
                              mergedGroupDocs,
                              totalMaxScore);
    }
  }
}
