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
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.LuceneTestCase;

public class TopGroupsTest extends LuceneTestCase {

    private final static SortField[] SORT_BY_SCORE = new SortField[]{SortField.FIELD_SCORE};

    private GroupDocs<String> mockEmptyGroup(String groupValue) {
        TotalHits totalHits = new TotalHits(0, TotalHits.Relation.EQUAL_TO);
        ScoreDoc[] scoreDocs = new ScoreDoc[0];
        return new GroupDocs<String>(Float.NaN, Float.NaN, totalHits, scoreDocs, groupValue, new Object[]{});
    }

    private GroupDocs<String>[] mockEmptyGroups(String[] groupValues) {
        GroupDocs<String>[] groups = new GroupDocs[groupValues.length];
        for (int groupId = 0; groupId < groups.length; groupId++) {
            groups[groupId] = mockEmptyGroup(groupValues[groupId]);
        }
        return groups;
    }

    private GroupDocs<String> mockGroup(int docid, float score, float maxScore, String groupValue) {
        TotalHits totalHits = new TotalHits(1, TotalHits.Relation.EQUAL_TO);
        ScoreDoc[] scoreDocs = new ScoreDoc[1];
        scoreDocs[0] = new ScoreDoc(docid, 2.0f);
        return new GroupDocs<String>(score, maxScore, totalHits, scoreDocs, groupValue, new Object[]{score});
    }

    private GroupDocs<String>[] mockGroups(int[] docids, float[] scores, float[] maxScores, String[] groupValues) {
        GroupDocs<String>[] groups = new GroupDocs[docids.length];
        for (int groupId = 0; groupId < docids.length; groupId++) {
            groups[groupId] = mockGroup(docids[groupId], scores[groupId], maxScores[groupId], groupValues[groupId]);
        }
        return groups;
    }

    public void testMaxScoreAfterMerge() {
        int totalHitCount = 0;
        int totalGroupedHitCount = 0;
        final String[] groupValues = new String[]{"group1", "group2"};
        // create results for the the first shard
        // docId 1 group1 score 5.0 max score 5.0
        // docId 2 group2 score 1.0 max score 1.0
        GroupDocs<String>[] groupDocs1 = mockGroups(/* doc ids */ new int[]{1, 2},
                /* scores */ new float[]{5.0f, 1.0f},
                /* max scores */ new float[]{5.0f, 1.0f},
                groupValues);

        TopGroups<String> topGroups1 = new TopGroups<>(SORT_BY_SCORE, SORT_BY_SCORE, totalHitCount, totalGroupedHitCount, groupDocs1, 5.0f);

        // create results for the second shard
        // docId 3 group1 score 3.0 max score 3.0
        // docId 4 group2 score 2.0 max score 2.0
        GroupDocs<String>[] groupDocs2 = mockGroups(/* doc ids */ new int[]{3, 4},
                /* scores */ new float[]{3.0f, 2.0f},
                /* max scores */ new float[]{3.0f, 2.0f},
                groupValues);
        TopGroups<String> topGroups2 = new TopGroups<>(SORT_BY_SCORE, SORT_BY_SCORE, totalHitCount, totalGroupedHitCount, groupDocs2, 2.0f);

        TopGroups[] shardGroups = new TopGroups[]{topGroups1, topGroups2};


        int topDocN = groupValues.length;
        int docOffset = 0;
        // retrieve the top groups
        TopGroups<String> mergedTopGroups = TopGroups.<String>merge(shardGroups, Sort.RELEVANCE, Sort.RELEVANCE, docOffset, topDocN, TopGroups.ScoreMergeMode.Total);
        assertEquals(5.0f, mergedTopGroups.maxScore, 0.0f);
        assertEquals(5.0f, mergedTopGroups.groups[0].maxScore, 0.0f); // max score for group1 will be docid 1 score 5
        assertEquals(2.0f, mergedTopGroups.groups[1].maxScore, 0.0f); // max score for group2 will be docid 4 score 2
    }

    public void testMaxScoreAfterhMergeWithNaN() {
        int totalHitCount = 0;
        int totalGroupedHitCount = 0;
        final String[] groupValues = new String[]{"group1", "group2"};

        // create results for the the first shard
        // docId 1 group1 score 5.0 max score 5.0
        // docId 2 group2 score 1.0 max score 5.0
        GroupDocs<String>[] groupDocs1 = mockGroups(/* doc ids */ new int[]{1, 2},
                /* scores */ new float[]{5.0f, 1.0f},
                /* max scores */ new float[]{5.0f, 1.0f},
                groupValues);

        TopGroups<String> topGroups = new TopGroups<>(SORT_BY_SCORE, SORT_BY_SCORE, totalHitCount, totalGroupedHitCount, groupDocs1, 5.0f);
        // create results for the second shard, empty
        GroupDocs<String>[] emptyGroups = mockEmptyGroups(groupValues);
        TopGroups emptyTopGroups = new TopGroups<>(SORT_BY_SCORE, SORT_BY_SCORE, totalHitCount, totalGroupedHitCount, emptyGroups, Float.NaN);

        TopGroups[] shardGroups = new TopGroups[]{topGroups, emptyTopGroups};

        int topDocN = groupValues.length;
        int docOffset = 0;
        // if we merge a group that has a valid maxScore with one that does not have it, we should get valid maxScore
        TopGroups<String> mergedTopGroups = TopGroups.<String>merge(shardGroups, Sort.RELEVANCE, Sort.RELEVANCE, docOffset, topDocN, TopGroups.ScoreMergeMode.Total);
        assertTrue(Float.isNaN(emptyTopGroups.maxScore));
        assertFalse(Float.isNaN(topGroups.maxScore));
        assertEquals(topGroups.maxScore, mergedTopGroups.maxScore, 0.0005f);
        assertEquals(5.0f, mergedTopGroups.groups[0].maxScore, 0.0f); // max score for group1 will be docid 1 score 5
        assertEquals(1.0f, mergedTopGroups.groups[1].maxScore, 0.0f); // max score for group1 will be docid 1 score 5

        // same if the groups come in different order
        TopGroups<String>[] reverseOrderTopGroups = new TopGroups[]{emptyTopGroups, topGroups};
        mergedTopGroups = TopGroups.<String>merge(reverseOrderTopGroups, Sort.RELEVANCE, Sort.RELEVANCE, docOffset, topDocN, TopGroups.ScoreMergeMode.Total);
        assertEquals(5.0f, mergedTopGroups.maxScore, 0.0);
        assertEquals(5.0f, mergedTopGroups.groups[0].maxScore, 0.0f); // max score for group1 will be docid 1 score 5
        assertEquals(1.0f, mergedTopGroups.groups[1].maxScore, 0.0f); // max score for group1 will be docid 1 score 5

        // if we merge empty groups that we should not have a max Score
        shardGroups = new TopGroups[]{emptyTopGroups, emptyTopGroups};
        mergedTopGroups = TopGroups.<String>merge(shardGroups, Sort.RELEVANCE, Sort.RELEVANCE, 0, 2, TopGroups.ScoreMergeMode.Total);
        assertTrue(Float.isNaN(mergedTopGroups.maxScore));

        // create results for the second shard
        // docId 3 group1 score 5.0 max score 5.0
        // docId 4 group2 score 1.0 max score 5.0
        GroupDocs<String>[] groupDocs2 = mockGroups(/* doc ids */ new int[]{3, 4},
                /* scores */ new float[]{3.0f, 2.0f},
                /* max scores */ new float[]{3.0f, 2.0f},
                groupValues);
        TopGroups<String> topGroups2 = new TopGroups<>(SORT_BY_SCORE, SORT_BY_SCORE, totalHitCount, totalGroupedHitCount, groupDocs2, 2.0f);

        // if we merge 3 groups, two with a valid maxScore (5.0, 2.0) and one without maxScore (NaN), then the final maxScore should be the maximum of the
        // two groups with valid maxScores (5.0)
        shardGroups = new TopGroups[]{emptyTopGroups, topGroups, topGroups2};
        mergedTopGroups = TopGroups.<String>merge(shardGroups, Sort.RELEVANCE, Sort.RELEVANCE, 0, 2, TopGroups.ScoreMergeMode.Total);
        assertEquals(5.0f, mergedTopGroups.maxScore, 0.0f);
        assertEquals(5.0f, mergedTopGroups.groups[0].maxScore, 0.0f); // max score for group1 will be docid 1 score 5
        assertEquals(2.0f, mergedTopGroups.groups[1].maxScore, 0.0f); // max score for group1 will be docid 4 score 2
    }
}
