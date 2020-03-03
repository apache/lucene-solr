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
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.LuceneTestCase;

public class TopGroupsTest extends LuceneTestCase {

  public void testAllGroupsEmptyInSecondPass() {
    narrativeMergeTestImplementation(false, false, false, false);
  }

  public void testSomeGroupsEmptyInSecondPass() {
    narrativeMergeTestImplementation(false, false, false, true);
    narrativeMergeTestImplementation(false, false, true, false);
    narrativeMergeTestImplementation(false, false, true, true);

    narrativeMergeTestImplementation(false, true, false, false);
    narrativeMergeTestImplementation(false, true, false, true);
    narrativeMergeTestImplementation(false, true, true, false);
    narrativeMergeTestImplementation(false, true, true, true);

    narrativeMergeTestImplementation(true, false, false, false);
    narrativeMergeTestImplementation(true, false, false, true);
    narrativeMergeTestImplementation(true, false, true, false);
    narrativeMergeTestImplementation(true, false, true, true);

    narrativeMergeTestImplementation(true, true, false, false);
    narrativeMergeTestImplementation(true, true, false, true);
    narrativeMergeTestImplementation(true, true, true, false);
  }

  public void testNoGroupsEmptyInSecondPass() {
    narrativeMergeTestImplementation(true, true, true, true);
  }

  /*
   * This method implements tests for the <code>TopGroup.merge</code> method
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
   *
   * Test permutations (haveBlueWhale, haveRedAnt, haveBlueDragonfly, haveRedSquirrel) arise because
   * in the first pass of the search all documents can be present, but
   * in the second pass of the search some documents could be missing
   * if they have been deleted 'just so' between the two phases.
   *
   * Additionally a <code>haveAnimal == false</code> condition also represents scenarios where a given
   * group has documents on some but not all shards in the collection.
   */
  private void narrativeMergeTestImplementation(
      boolean haveBlueWhale,
      boolean haveRedAnt,
      boolean haveBlueDragonfly,
      boolean haveRedSquirrel) {

    final String blueGroupValue = "blue";
    final String redGroupValue = "red";

    final Integer redAntSize = 1;
    final Integer blueDragonflySize = 10;
    final Integer redSquirrelSize = 100;
    final Integer blueWhaleSize = 1000;

    final float redAntScore = redAntSize;
    final float blueDragonflyScore = blueDragonflySize;
    final float redSquirrelScore = redSquirrelSize;
    final float blueWhaleScore = blueWhaleSize;

    final Sort sort = Sort.RELEVANCE;

    final TopGroups<String> shard1TopGroups;
    {
      final GroupDocs<String> group1 = haveBlueWhale
          ? createSingletonGroupDocs(blueGroupValue, new Object[] { blueWhaleSize }, 1 /* docId */, blueWhaleScore, 0 /* shardIndex */)
              : createEmptyGroupDocs(blueGroupValue, new Object[] { blueWhaleSize });

      final GroupDocs<String> group2 = haveRedAnt
          ? createSingletonGroupDocs(redGroupValue, new Object[] { redAntSize }, 2 /* docId */, redAntScore, 0 /* shardIndex */)
              : createEmptyGroupDocs(redGroupValue, new Object[] { redAntSize });

      shard1TopGroups = new TopGroups<String>(
          sort.getSort() /* groupSort */,
          sort.getSort() /* withinGroupSort */,
          group1.scoreDocs.length + group2.scoreDocs.length /* totalHitCount */,
          group1.scoreDocs.length + group2.scoreDocs.length /* totalGroupedHitCount */,
          combineGroupDocs(group1, group2) /* groups */,
          (haveBlueWhale ? blueWhaleScore : (haveRedAnt ? redAntScore : Float.NaN)) /* maxScore */);
    }

    final TopGroups<String> shard2TopGroups;
    {
      final GroupDocs<String> group1 = haveBlueDragonfly
          ? createSingletonGroupDocs(blueGroupValue, new Object[] { blueDragonflySize }, 3 /* docId */, blueDragonflyScore, 1 /* shardIndex */)
              : createEmptyGroupDocs(blueGroupValue, new Object[] { blueDragonflySize });

      final GroupDocs<String> group2 = haveRedSquirrel
      ? createSingletonGroupDocs(redGroupValue, new Object[] { redSquirrelSize }, 4 /* docId */, redSquirrelScore, 1 /* shardIndex */)
          : createEmptyGroupDocs(redGroupValue, new Object[] { redSquirrelSize });

      shard2TopGroups = new TopGroups<String>(
          sort.getSort() /* groupSort */,
          sort.getSort() /* withinGroupSort */,
          group1.scoreDocs.length + group2.scoreDocs.length /* totalHitCount */,
          group1.scoreDocs.length + group2.scoreDocs.length /* totalGroupedHitCount */,
          combineGroupDocs(group1, group2) /* groups */,
          (haveRedSquirrel ? redSquirrelScore : (haveBlueDragonfly ? blueDragonflyScore : Float.NaN)) /* maxScore */);
    }

    final TopGroups<String> mergedTopGroups = TopGroups.<String>merge(
        combineTopGroups(shard1TopGroups, shard2TopGroups),
        sort /* groupSort */,
        sort /* docSort */,
        0 /* docOffset */,
        2 /* docTopN */,
        TopGroups.ScoreMergeMode.None);
    assertNotNull(mergedTopGroups);

    final int expectedCount =
        (haveBlueWhale     ? 1 : 0) +
        (haveRedAnt        ? 1 : 0) +
        (haveBlueDragonfly ? 1 : 0) +
        (haveRedSquirrel   ? 1 : 0);

    assertEquals(expectedCount, mergedTopGroups.totalHitCount);
    assertEquals(expectedCount, mergedTopGroups.totalGroupedHitCount);

    assertEquals(2, mergedTopGroups.groups.length);
    {
      assertEquals(blueGroupValue, mergedTopGroups.groups[0].groupValue);
      final float expectedBlueMaxScore =
          (haveBlueWhale ? blueWhaleScore : (haveBlueDragonfly ? blueDragonflyScore : Float.NaN));
      checkMaxScore(expectedBlueMaxScore, mergedTopGroups.groups[0].maxScore);
    }
    {
      assertEquals(redGroupValue, mergedTopGroups.groups[1].groupValue);
      final float expectedRedMaxScore =
          (haveRedSquirrel ? redSquirrelScore : (haveRedAnt ? redAntScore : Float.NaN));
      checkMaxScore(expectedRedMaxScore, mergedTopGroups.groups[1].maxScore);
    }

    final float expectedMaxScore =
        (haveBlueWhale ? blueWhaleScore
            : (haveRedSquirrel ? redSquirrelScore
                : (haveBlueDragonfly ? blueDragonflyScore
                    : (haveRedAnt ? redAntScore
                        : Float.NaN))));
    checkMaxScore(expectedMaxScore, mergedTopGroups.maxScore);
  }

  private static void checkMaxScore(float expected, float actual) {
    if (Float.isNaN(expected)) {
      assertTrue(Float.isNaN(actual));
    } else {
      assertEquals(expected, actual, 0.0);
    }
  }

  // helper methods

  private static GroupDocs<String> createEmptyGroupDocs(String groupValue, Object[] groupSortValues) {
    return new  GroupDocs<String>(
        Float.NaN /* score */,
        Float.NaN /* maxScore */,
        new TotalHits(0, TotalHits.Relation.EQUAL_TO),
        new ScoreDoc[0],
        groupValue,
        groupSortValues);
    }

  private static GroupDocs<String> createSingletonGroupDocs(String groupValue, Object[] groupSortValues,
      int docId, float docScore, int shardIndex) {
    return new  GroupDocs<String>(
        Float.NaN /* score */,
        docScore /* maxScore */,
        new TotalHits(1, TotalHits.Relation.EQUAL_TO),
        new ScoreDoc[] { new ScoreDoc(docId, docScore, shardIndex) },
        groupValue,
        groupSortValues);
    }

  private static GroupDocs<String>[] combineGroupDocs(GroupDocs<String> group0, GroupDocs<String> group1) {
    @SuppressWarnings({"unchecked","rawtypes"})
    final GroupDocs<String>[] groups = new GroupDocs[2];
    groups[0] = group0;
    groups[1] = group1;
    return groups;
  }

  private static TopGroups<String>[] combineTopGroups(TopGroups<String> group0, TopGroups<String> group1) {
    @SuppressWarnings({"unchecked","rawtypes"})
    final TopGroups<String>[] groups = new TopGroups[2];
    groups[0] = group0;
    groups[1] = group1;
    return groups;
  }

}
