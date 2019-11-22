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

import java.util.Arrays;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Ignore;
import org.junit.Test;

/*
 * This class implements tests for the <code>TopGroup.merge</code> method
 * using a narrative approach. Use of a creative narrative may seem unusual
 * or even silly but the idea behind it is to make it hopefully easier to
 * reason about the documents and groups and scores in the test whilst testing
 * several scenario permutations.
 *
 * Imagine:
 *
 * Each document represents (say) a picture book of an animal.
 * We are searching for two books and wish to draw a picture of our own, inspired by the books.
 * We think that large animals are easier to draw and therefore order the books by the featured animal's size.
 * We think that different colors would make for a good drawing and therefore group the books by the featured animal's color.
 *
 * Index content:
 *
 * The documents are in 2 groups ("blue" and "red") and there are 4 documents across 2 shards:
 * shard 1 (blue whale, red ant) and shard 2 (blue dragonfly, red squirrel).
 *
 * If all documents are present the "blue whale" and the "red squirrel" documents would be returned
 * for our drawing since they are the largest animals in their respective groups.
 */
public class TopGroupsTest extends LuceneTestCase {

  private final static String RED_GROUP_KEY = "RED";
  private final static String BLUE_GROUP_KEY = "BLUE";

  private final static Sort GROUP_SORT = Sort.RELEVANCE;
  private final static Sort DOC_SORT = Sort.RELEVANCE;

  private final static int DOC_OFFSET = 0;
  private final static int TOP_DOC_N = 10;


  private final static ScoreDoc DOC_RED_ANT = new ScoreDoc(1 /* docid */, 1.0f /* score */);
  private final static ScoreDoc DOC_RED_SQUIRREL = new ScoreDoc(3 /* docid */, 3.0f /* score */);
  private final static ScoreDoc DOC_BLUE_DRAGONFLY = new ScoreDoc(2 /* docid */, 2.0f /* score */);
  private final static ScoreDoc DOC_BLUE_WHALE = new ScoreDoc(4 /* docid */, 4.0f /* score */);

  private final static TopGroups.ScoreMergeMode MERGE_MODE = TopGroups.ScoreMergeMode.Total;

  @Test
  public void testAllGroupsEmptyInSecondPass() {
    final GroupDocs<String> emptyRed = createEmptyGroupDocs(RED_GROUP_KEY);
    final GroupDocs<String> emptyBlue = createEmptyGroupDocs(BLUE_GROUP_KEY);

    final TopGroups<String> emptyGroups = createTopGroups(groupResults(emptyRed, emptyBlue), Float.NaN);

    // if we merge two empty groups the final maxScore should be Float.NaN
    final TopGroups<String> mergedGroups = TopGroups.merge(shardResponses(emptyGroups, emptyGroups), GROUP_SORT, DOC_SORT, DOC_OFFSET, TOP_DOC_N, MERGE_MODE);
    assertNotNull("Merged groups cannot be null", mergedGroups);
    assertTrue("Score should be equal to Float.NaN when merging empty top groups", Float.isNaN(mergedGroups.maxScore));
  }

  @Test
  @Ignore("Ignore until https://issues.apache.org/jira/browse/LUCENE-8996 is resolved")
  public void testMergeARealScoreWithNanShouldntReturnNaN() {
    final GroupDocs<String> emptyRed = createEmptyGroupDocs(RED_GROUP_KEY);
    final GroupDocs<String> emptyBlue = createEmptyGroupDocs(BLUE_GROUP_KEY);

    final GroupDocs<String> fullRed = createGroupDocs(RED_GROUP_KEY, new ScoreDoc[]{DOC_RED_ANT, DOC_RED_SQUIRREL}, DOC_RED_SQUIRREL.score /*score*/, DOC_RED_SQUIRREL.score /*max score*/);
    final GroupDocs<String> fullBlue = createGroupDocs(BLUE_GROUP_KEY, new ScoreDoc[]{DOC_BLUE_DRAGONFLY, DOC_BLUE_WHALE}, DOC_BLUE_WHALE.score /*score*/, DOC_BLUE_WHALE.score /*max score*/);

    final float maxScoreShard1 = fullBlue.maxScore;
    // shard one has real groups with a max score
    final TopGroups<String> topGroupsShard1 = createTopGroups(groupResults(fullBlue, fullRed), maxScoreShard1);
    // shard two has empty groups with max score Float.NaN
    final TopGroups<String> topGroupsShard2 = createTopGroups(groupResults(emptyBlue, emptyRed), Float.NaN);

    // merging the groups must produce a TopGroups object with maxScore equals to maxScoreShard1
    final TopGroups<String> mergedGroups = TopGroups.merge(shardResponses(topGroupsShard1, topGroupsShard2), GROUP_SORT, DOC_SORT, DOC_OFFSET, TOP_DOC_N, MERGE_MODE);
    assertNotNull("Merged groups cannot be null", mergedGroups);
    assertEquals(maxScoreShard1, mergedGroups.maxScore, 0.0);
  }

  @Test
  public void testMergeARealScoresReturnTheRightResults() {
    final GroupDocs<String> redSquirrelGroupDocs = createGroupDocs(RED_GROUP_KEY, new ScoreDoc[]{DOC_RED_SQUIRREL}, DOC_RED_SQUIRREL.score /*score*/, DOC_RED_SQUIRREL.score /*max score*/);
    final GroupDocs<String> blueWhaleGroupDocs = createGroupDocs(BLUE_GROUP_KEY, new ScoreDoc[]{DOC_BLUE_WHALE}, DOC_BLUE_WHALE.score /*score*/, DOC_BLUE_WHALE.score /*max score*/);
    final GroupDocs<String> blueDragonflyGroupDocs = createGroupDocs(BLUE_GROUP_KEY, new ScoreDoc[]{DOC_BLUE_DRAGONFLY}, DOC_BLUE_DRAGONFLY.score /*score*/, DOC_BLUE_DRAGONFLY.score /*max score*/);
    final GroupDocs<String> redAntGroupDocs = createGroupDocs(RED_GROUP_KEY, new ScoreDoc[]{DOC_RED_ANT}, DOC_RED_ANT.score /*score*/, DOC_RED_ANT.score /*max score*/);

    // shard one has real groups with a max score
    final TopGroups<String> topGroupsShard1 = createTopGroups(groupResults(blueWhaleGroupDocs, redAntGroupDocs), blueWhaleGroupDocs.maxScore);
    // shard two has empty groups with max score Float.NaN
    final TopGroups<String> topGroupsShard2 = createTopGroups(groupResults(blueDragonflyGroupDocs, redSquirrelGroupDocs), blueDragonflyGroupDocs.maxScore);

    // merging the groups should produce a group that has blueWhaleGroupDocs.max as a maxScore
    final TopGroups<String> mergedGroups = TopGroups.merge(shardResponses(topGroupsShard1, topGroupsShard2), GROUP_SORT, DOC_SORT, DOC_OFFSET, TOP_DOC_N, MERGE_MODE);
    assertNotNull("Merged groups cannot be null", mergedGroups);
    assertEquals(blueWhaleGroupDocs.maxScore, mergedGroups.maxScore, 0.0);
    // also the first group should be blue and contain the biggest blue animal (the whale)
    assertEquals(DOC_BLUE_WHALE.doc, mergedGroups.groups[0].scoreDocs[0].doc);
    // the second group should be red and contain the biggest red animal (the squirrel)
    assertEquals(DOC_RED_SQUIRREL.doc, mergedGroups.groups[1].scoreDocs[0].doc);
  }

  // helper methods

  // takes a pair of groups and returns them in an array
  private static GroupDocs<String>[] groupResults(GroupDocs<String> group1, GroupDocs<String> group2){
    @SuppressWarnings("unchecked")
     GroupDocs<String>[] groupDocs = (GroupDocs<String>[])new GroupDocs<?>[2];
     groupDocs[0] = group1;
     groupDocs[1] = group2;
     return groupDocs;
  }

  // takes a pair of shard responses and returns an array
  private static TopGroups<String>[] shardResponses(TopGroups<String> topGroups1, TopGroups<String> topGroups2){
    @SuppressWarnings("unchecked")
    TopGroups<String>[] responses = (TopGroups<String>[])new TopGroups<?>[2];
    responses[0] = topGroups1;
    responses[1] = topGroups2;
    return responses;
  }

  /* Create a TopGroup containing the given GroupDocs - sort will be by score and total hit count and group hit
   * count will be randomized values */
  private static <T> TopGroups<T> createTopGroups(GroupDocs<T>[] groups, float maxScore) {
    final SortField[] sortByScore = new SortField[]{SortField.FIELD_SCORE};
    final int totalHitCount = random().nextInt(1000);
    final int totalGroupedHitCount = random().nextInt(totalHitCount);
    return new TopGroups<>(sortByScore, sortByScore, totalHitCount, totalGroupedHitCount, groups, maxScore);
  }

  /* Create a GroupDocs with no documents, the response sent by a shard that did not have documents
   matching the given group */
  private static GroupDocs<String> createEmptyGroupDocs(String groupValue) {
    final Object[] groupSortValues = new Object[0]; // empty: no groupSortValues
    return new GroupDocs<>(
        Float.NaN /* score */,
        Float.NaN /* maxScore */,
        new TotalHits(0, TotalHits.Relation.EQUAL_TO),
        new ScoreDoc[0],
        groupValue,
        groupSortValues);
  }

  /* Create a GroupDocs with the given an array of ScoreDocs that belong to the group. It assumes
   * that we are sorting only on scores and fetches the scores from the given ScoreDocs  */
  private static GroupDocs<String> createGroupDocs(String groupValue, ScoreDoc[] scoreDocs, float score, float maxScore) {
    // extract the scores from the scoreDocs
    final Object[] groupSortValues = Arrays.stream(scoreDocs).map(doc -> doc.score).toArray();
    return new GroupDocs<>(
        score,
        maxScore,
        new TotalHits(0, TotalHits.Relation.EQUAL_TO),
        scoreDocs,
        groupValue,
        groupSortValues);
  }
}
