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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;

import java.io.IOException;
import java.util.*;

/**
 * A base implementation of {@link AbstractAllGroupHeadsCollector} for retrieving the most relevant groups when grouping
 * on a string based group field. More specifically this all concrete implementations of this base implementation
 * use {@link org.apache.lucene.search.FieldCache.StringIndex}.
 *
 * @lucene.experimental
 */
public abstract class TermAllGroupHeadsCollector<GH extends AbstractAllGroupHeadsCollector.GroupHead> extends AbstractAllGroupHeadsCollector<GH> {

  private static final int DEFAULT_INITIAL_SIZE = 128;

  final String groupField;
  FieldCache.StringIndex groupIndex;
  IndexReader indexReader;
  int docBase;

  protected TermAllGroupHeadsCollector(String groupField, int numberOfSorts) {
    super(numberOfSorts);
    this.groupField = groupField;
  }

  /**
   * Creates an <code>AbstractAllGroupHeadsCollector</code> instance based on the supplied arguments.
   * This factory method decides with implementation is best suited.
   *
   * Delegates to {@link #create(String, org.apache.lucene.search.Sort, int)} with an initialSize of 128.
   *
   * @param groupField      The field to group by
   * @param sortWithinGroup The sort within each group
   * @return an <code>AbstractAllGroupHeadsCollector</code> instance based on the supplied arguments
   * @throws IOException If I/O related errors occur
   */
  public static AbstractAllGroupHeadsCollector create(String groupField, Sort sortWithinGroup) throws IOException {
    return create(groupField, sortWithinGroup, DEFAULT_INITIAL_SIZE);
  }

  /**
   * Creates an <code>AbstractAllGroupHeadsCollector</code> instance based on the supplied arguments.
   * This factory method decides with implementation is best suited.
   *
   * @param groupField      The field to group by
   * @param sortWithinGroup The sort within each group
   * @param initialSize The initial allocation size of the internal int set and group list which should roughly match
   *                    the total number of expected unique groups. Be aware that the heap usage is
   *                    4 bytes * initialSize.
   * @return an <code>AbstractAllGroupHeadsCollector</code> instance based on the supplied arguments
   * @throws IOException If I/O related errors occur
   */
  public static AbstractAllGroupHeadsCollector create(String groupField, Sort sortWithinGroup, int initialSize) throws IOException {
    boolean sortAllScore = true;
    boolean sortAllFieldValue = true;

    for (SortField sortField : sortWithinGroup.getSort()) {
      if (sortField.getType() == SortField.SCORE) {
        sortAllFieldValue = false;
      } else if (needGeneralImpl(sortField)) {
        return new GeneralAllGroupHeadsCollector(groupField, sortWithinGroup);
      } else {
        sortAllScore = false;
      }
    }

    if (sortAllScore) {
      return new ScoreAllGroupHeadsCollector(groupField, sortWithinGroup, initialSize);
    } else if (sortAllFieldValue) {
      return new OrdAllGroupHeadsCollector(groupField, sortWithinGroup, initialSize);
    } else {
      return new OrdScoreAllGroupHeadsCollector(groupField, sortWithinGroup, initialSize);
    }
  }

  // Returns when a sort field needs the general impl.
  private static boolean needGeneralImpl(SortField sortField) {
    int sortType = sortField.getType();
    // Note (MvG): We can also make an optimized impl when sorting is SortField.DOC
    return sortType != SortField.STRING_VAL && sortType != SortField.STRING && sortType != SortField.SCORE;
  }

  // A general impl that works for any group sort.
  static class GeneralAllGroupHeadsCollector extends TermAllGroupHeadsCollector<GeneralAllGroupHeadsCollector.GroupHead> {

    private final Sort sortWithinGroup;
    private final Map<String, GroupHead> groups;

    private Scorer scorer;

    GeneralAllGroupHeadsCollector(String groupField, Sort sortWithinGroup) throws IOException {
      super(groupField, sortWithinGroup.getSort().length);
      this.sortWithinGroup = sortWithinGroup;
      groups = new HashMap<String, GroupHead>();

      final SortField[] sortFields = sortWithinGroup.getSort();
      for (int i = 0; i < sortFields.length; i++) {
        reversed[i] = sortFields[i].getReverse() ? -1 : 1;
      }
    }

    protected void retrieveGroupHeadAndAddIfNotExist(int doc) throws IOException {
      final int ord = groupIndex.order[doc];
      final String groupValue = ord == 0 ? null : groupIndex.lookup[ord];
      GroupHead groupHead = groups.get(groupValue);
      if (groupHead == null) {
        groupHead = new GroupHead(groupValue, sortWithinGroup, doc);
        groups.put(groupValue == null ? null : groupValue, groupHead);
        temporalResult.stop = true;
      } else {
        temporalResult.stop = false;
      }
      temporalResult.groupHead = groupHead;
    }

    protected Collection<GroupHead> getCollectedGroupHeads() {
      return groups.values();
    }

    public void setNextReader(IndexReader reader, int docBase) throws IOException {
      this.indexReader = reader;
      this.docBase = docBase;
      groupIndex = FieldCache.DEFAULT.getStringIndex(reader, groupField);

      for (GroupHead groupHead : groups.values()) {
        for (int i = 0; i < groupHead.comparators.length; i++) {
          groupHead.comparators[i].setNextReader(reader, docBase);
        }
      }
    }

    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
      for (GroupHead groupHead : groups.values()) {
        for (FieldComparator comparator : groupHead.comparators) {
          comparator.setScorer(scorer);
        }
      }
    }

    class GroupHead extends AbstractAllGroupHeadsCollector.GroupHead<String> {

      final FieldComparator[] comparators;

      private GroupHead(String groupValue, Sort sort, int doc) throws IOException {
        super(groupValue, doc + docBase);
        final SortField[] sortFields = sort.getSort();
        comparators = new FieldComparator[sortFields.length];
        for (int i = 0; i < sortFields.length; i++) {
          comparators[i] = sortFields[i].getComparator(1, i);
          comparators[i].setNextReader(indexReader, docBase);
          comparators[i].setScorer(scorer);
          comparators[i].copy(0, doc);
          comparators[i].setBottom(0);
        }
      }

      public int compare(int compIDX, int doc) throws IOException {
        return comparators[compIDX].compareBottom(doc);
      }

      public void updateDocHead(int doc) throws IOException {
        for (FieldComparator comparator : comparators) {
          comparator.copy(0, doc);
          comparator.setBottom(0);
        }
        this.doc = doc + docBase;
      }
    }
  }


  // AbstractAllGroupHeadsCollector optimized for ord fields and scores.
  static class OrdScoreAllGroupHeadsCollector extends TermAllGroupHeadsCollector<OrdScoreAllGroupHeadsCollector.GroupHead> {

    private final SentinelIntSet ordSet;
    private final List<GroupHead> collectedGroups;
    private final SortField[] fields;

    private FieldCache.StringIndex[] sortsIndex;
    private Scorer scorer;
    private GroupHead[] segmentGroupHeads;

    OrdScoreAllGroupHeadsCollector(String groupField, Sort sortWithinGroup, int initialSize) {
      super(groupField, sortWithinGroup.getSort().length);
      ordSet = new SentinelIntSet(initialSize, -1);
      collectedGroups = new ArrayList<GroupHead>(initialSize);

      final SortField[] sortFields = sortWithinGroup.getSort();
      fields = new SortField[sortFields.length];
      sortsIndex = new FieldCache.StringIndex[sortFields.length];
      for (int i = 0; i < sortFields.length; i++) {
        reversed[i] = sortFields[i].getReverse() ? -1 : 1;
        fields[i] = sortFields[i];
      }
    }

    protected Collection<GroupHead> getCollectedGroupHeads() {
      return collectedGroups;
    }

    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
    }

    protected void retrieveGroupHeadAndAddIfNotExist(int doc) throws IOException {
      int key = groupIndex.order[doc];
      GroupHead groupHead;
      if (!ordSet.exists(key)) {
        ordSet.put(key);
        String term = key == 0 ? null : groupIndex.lookup[key];
        groupHead = new GroupHead(doc, term);
        collectedGroups.add(groupHead);
        segmentGroupHeads[key] = groupHead;
        temporalResult.stop = true;
      } else {
        temporalResult.stop = false;
        groupHead = segmentGroupHeads[key];
      }
      temporalResult.groupHead = groupHead;
    }

    public void setNextReader(IndexReader reader, int docBase) throws IOException {
      this.indexReader = reader;
      this.docBase = docBase;
      groupIndex = FieldCache.DEFAULT.getStringIndex(reader, groupField);
      for (int i = 0; i < fields.length; i++) {
        if (fields[i].getType() == SortField.SCORE) {
          continue;
        }

        sortsIndex[i] = FieldCache.DEFAULT.getStringIndex(reader, fields[i].getField());
      }

      // Clear ordSet and fill it with previous encountered groups that can occur in the current segment.
      ordSet.clear();
      segmentGroupHeads = new GroupHead[groupIndex.lookup.length];
      for (GroupHead collectedGroup : collectedGroups) {
        int ord = groupIndex.binarySearchLookup(collectedGroup.groupValue);
        if (ord >= 0) {
          ordSet.put(ord);
          segmentGroupHeads[ord] = collectedGroup;

          for (int i = 0; i < sortsIndex.length; i++) {
            if (fields[i].getType() == SortField.SCORE) {
              continue;
            }

            collectedGroup.sortOrds[i] = sortsIndex[i].binarySearchLookup(collectedGroup.sortValues[i]);
          }
        }
      }
    }

    class GroupHead extends AbstractAllGroupHeadsCollector.GroupHead<String> {

      String[] sortValues;
      int[] sortOrds;
      float[] scores;

      private GroupHead(int doc, String groupValue) throws IOException {
        super(groupValue, doc + docBase);
        sortValues = new String[sortsIndex.length];
        sortOrds = new int[sortsIndex.length];
        scores = new float[sortsIndex.length];
        for (int i = 0; i < sortsIndex.length; i++) {
          if (fields[i].getType() == SortField.SCORE) {
            scores[i] = scorer.score();
          } else {
            sortValues[i] = sortsIndex[i].lookup[sortsIndex[i].order[doc]];
            sortOrds[i] = sortsIndex[i].order[doc];
          }
        }

      }

      public int compare(int compIDX, int doc) throws IOException {
        if (fields[compIDX].getType() == SortField.SCORE) {
          float score = scorer.score();
          if (scores[compIDX] < score) {
            return 1;
          } else if (scores[compIDX] > score) {
            return -1;
          }
          return 0;
        } else {
          if (sortOrds[compIDX] < 0) {
            // The current segment doesn't contain the sort value we encountered before. Therefore the ord is negative.
            final String val1 = sortValues[compIDX];
            final String val2 = sortsIndex[compIDX].lookup[sortsIndex[compIDX].order[doc]];
            if (val1 == null) {
              if (val2 == null) {
                return 0;
              }
              return -1;
            } else if (val2 == null) {
              return 1;
            }
            return val1.compareTo(val2);
          } else {
            return sortOrds[compIDX] - sortsIndex[compIDX].order[doc];
          }
        }
      }

      public void updateDocHead(int doc) throws IOException {
        for (int i = 0; i < sortsIndex.length; i++) {
          if (fields[i].getType() == SortField.SCORE) {
            scores[i] = scorer.score();
          } else {
            sortValues[i] = sortsIndex[i].lookup[sortsIndex[i].order[doc]];
            sortOrds[i] = sortsIndex[i].order[doc];
          }
        }
        this.doc = doc + docBase;
      }

    }

  }


  // AbstractAllGroupHeadsCollector optimized for ord fields.
  static class OrdAllGroupHeadsCollector extends TermAllGroupHeadsCollector<OrdAllGroupHeadsCollector.GroupHead> {

    private final SentinelIntSet ordSet;
    private final List<GroupHead> collectedGroups;
    private final SortField[] fields;

    private FieldCache.StringIndex[] sortsIndex;
    private GroupHead[] segmentGroupHeads;

    OrdAllGroupHeadsCollector(String groupField, Sort sortWithinGroup, int initialSize) {
      super(groupField, sortWithinGroup.getSort().length);
      ordSet = new SentinelIntSet(initialSize, -1);
      collectedGroups = new ArrayList<GroupHead>(initialSize);

      final SortField[] sortFields = sortWithinGroup.getSort();
      fields = new SortField[sortFields.length];
      sortsIndex = new FieldCache.StringIndex[sortFields.length];
      for (int i = 0; i < sortFields.length; i++) {
        reversed[i] = sortFields[i].getReverse() ? -1 : 1;
        fields[i] = sortFields[i];
      }
    }

    protected Collection<GroupHead> getCollectedGroupHeads() {
      return collectedGroups;
    }

    public void setScorer(Scorer scorer) throws IOException {
    }

    protected void retrieveGroupHeadAndAddIfNotExist(int doc) throws IOException {
      int key = groupIndex.order[doc];
      GroupHead groupHead;
      if (!ordSet.exists(key)) {
        ordSet.put(key);
        String term = key == 0 ? null : groupIndex.lookup[key];
        groupHead = new GroupHead(doc, term);
        collectedGroups.add(groupHead);
        segmentGroupHeads[key] = groupHead;
        temporalResult.stop = true;
      } else {
        temporalResult.stop = false;
        groupHead = segmentGroupHeads[key];
      }
      temporalResult.groupHead = groupHead;
    }

    public void setNextReader(IndexReader reader, int docBase) throws IOException {
      this.indexReader = reader;
      this.docBase = docBase;
      groupIndex = FieldCache.DEFAULT.getStringIndex(reader, groupField);
      for (int i = 0; i < fields.length; i++) {
        sortsIndex[i] = FieldCache.DEFAULT.getStringIndex(reader, fields[i].getField());
      }

      // Clear ordSet and fill it with previous encountered groups that can occur in the current segment.
      ordSet.clear();
      segmentGroupHeads = new GroupHead[groupIndex.lookup.length];
      for (GroupHead collectedGroup : collectedGroups) {
        int groupOrd = groupIndex.binarySearchLookup(collectedGroup.groupValue);
        if (groupOrd >= 0) {
          ordSet.put(groupOrd);
          segmentGroupHeads[groupOrd] = collectedGroup;

          for (int i = 0; i < sortsIndex.length; i++) {
            collectedGroup.sortOrds[i] = sortsIndex[i].binarySearchLookup(collectedGroup.sortValues[i]);
          }
        }
      }
    }

    class GroupHead extends AbstractAllGroupHeadsCollector.GroupHead<String> {

      String[] sortValues;
      int[] sortOrds;

      private GroupHead(int doc, String groupValue) throws IOException {
        super(groupValue, doc + docBase);
        sortValues = new String[sortsIndex.length];
        sortOrds = new int[sortsIndex.length];
        for (int i = 0; i < sortsIndex.length; i++) {
          sortValues[i] = sortsIndex[i].lookup[sortsIndex[i].order[doc]];
          sortOrds[i] = sortsIndex[i].order[doc];
        }
      }

      public int compare(int compIDX, int doc) throws IOException {
        if (sortOrds[compIDX] < 0) {
          // The current segment doesn't contain the sort value we encountered before. Therefore the ord is negative.
          final String val1 = sortValues[compIDX];
          final String val2 = sortsIndex[compIDX].lookup[sortsIndex[compIDX].order[doc]];
          if (val1 == null) {
            if (val2 == null) {
              return 0;
            }
            return -1;
          } else if (val2 == null) {
            return 1;
          }
          return val1.compareTo(val2);
        } else {
          return sortOrds[compIDX] - sortsIndex[compIDX].order[doc];
        }
      }

      public void updateDocHead(int doc) throws IOException {
        for (int i = 0; i < sortsIndex.length; i++) {
          sortValues[i] = sortsIndex[i].lookup[sortsIndex[i].order[doc]];
          sortOrds[i] = sortsIndex[i].order[doc];
        }
        this.doc = doc + docBase;
      }

    }

  }


  // AbstractAllGroupHeadsCollector optimized for scores.
  static class ScoreAllGroupHeadsCollector extends TermAllGroupHeadsCollector<ScoreAllGroupHeadsCollector.GroupHead> {

    private final SentinelIntSet ordSet;
    private final List<GroupHead> collectedGroups;
    private final SortField[] fields;

    private Scorer scorer;
    private GroupHead[] segmentGroupHeads;

    ScoreAllGroupHeadsCollector(String groupField, Sort sortWithinGroup, int initialSize) {
      super(groupField, sortWithinGroup.getSort().length);
      ordSet = new SentinelIntSet(initialSize, -1);
      collectedGroups = new ArrayList<GroupHead>(initialSize);

      final SortField[] sortFields = sortWithinGroup.getSort();
      fields = new SortField[sortFields.length];
      for (int i = 0; i < sortFields.length; i++) {
        reversed[i] = sortFields[i].getReverse() ? -1 : 1;
        fields[i] = sortFields[i];
      }
    }

    protected Collection<GroupHead> getCollectedGroupHeads() {
      return collectedGroups;
    }

    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
    }

    protected void retrieveGroupHeadAndAddIfNotExist(int doc) throws IOException {
      int key = groupIndex.order[doc];
      GroupHead groupHead;
      if (!ordSet.exists(key)) {
        ordSet.put(key);
        String term = key == 0 ? null : groupIndex.lookup[key];
        groupHead = new GroupHead(doc, term);
        collectedGroups.add(groupHead);
        segmentGroupHeads[key] = groupHead;
        temporalResult.stop = true;
      } else {
        temporalResult.stop = false;
        groupHead = segmentGroupHeads[key];
      }
      temporalResult.groupHead = groupHead;
    }

    public void setNextReader(IndexReader reader, int docBase) throws IOException {
      this.indexReader = reader;
      this.docBase = docBase;
      groupIndex = FieldCache.DEFAULT.getStringIndex(reader, groupField);

      // Clear ordSet and fill it with previous encountered groups that can occur in the current segment.
      ordSet.clear();
      segmentGroupHeads = new GroupHead[groupIndex.lookup.length];
      for (GroupHead collectedGroup : collectedGroups) {
        int ord = groupIndex.binarySearchLookup(collectedGroup.groupValue);
        if (ord >= 0) {
          ordSet.put(ord);
          segmentGroupHeads[ord] = collectedGroup;
        }
      }
    }

    class GroupHead extends AbstractAllGroupHeadsCollector.GroupHead<String> {

      float[] scores;

      private GroupHead(int doc, String groupValue) throws IOException {
        super(groupValue, doc + docBase);
        scores = new float[fields.length];
        float score = scorer.score();
        for (int i = 0; i < scores.length; i++) {
          scores[i] = score;
        }
      }

      public int compare(int compIDX, int doc) throws IOException {
        float score = scorer.score();
        if (scores[compIDX] < score) {
          return 1;
        } else if (scores[compIDX] > score) {
          return -1;
        }
        return 0;
      }

      public void updateDocHead(int doc) throws IOException {
        float score = scorer.score();
        for (int i = 0; i < scores.length; i++) {
          scores[i] = score;
        }
        this.doc = doc + docBase;
      }

    }

  }

}
