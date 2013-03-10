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
import java.util.*;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.*;
import org.apache.lucene.search.grouping.AbstractAllGroupHeadsCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SentinelIntSet;

/**
 * A base implementation of {@link org.apache.lucene.search.grouping.AbstractAllGroupHeadsCollector} for retrieving the most relevant groups when grouping
 * on a string based group field. More specifically this all concrete implementations of this base implementation
 * use {@link org.apache.lucene.index.SortedDocValues}.
 *
 * @lucene.experimental
 */
public abstract class TermAllGroupHeadsCollector<GH extends AbstractAllGroupHeadsCollector.GroupHead<?>> extends AbstractAllGroupHeadsCollector<GH> {

  private static final int DEFAULT_INITIAL_SIZE = 128;

  final String groupField;
  final BytesRef scratchBytesRef = new BytesRef();

  SortedDocValues groupIndex;
  AtomicReaderContext readerContext;

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
   */
  public static AbstractAllGroupHeadsCollector<?> create(String groupField, Sort sortWithinGroup) {
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
   */
  public static AbstractAllGroupHeadsCollector<?> create(String groupField, Sort sortWithinGroup, int initialSize) {
    boolean sortAllScore = true;
    boolean sortAllFieldValue = true;

    for (SortField sortField : sortWithinGroup.getSort()) {
      if (sortField.getType() == SortField.Type.SCORE) {
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
    SortField.Type sortType = sortField.getType();
    // Note (MvG): We can also make an optimized impl when sorting is SortField.DOC
    return sortType != SortField.Type.STRING_VAL && sortType != SortField.Type.STRING && sortType != SortField.Type.SCORE;
  }

  // A general impl that works for any group sort.
  static class GeneralAllGroupHeadsCollector extends TermAllGroupHeadsCollector<GeneralAllGroupHeadsCollector.GroupHead> {

    private final Sort sortWithinGroup;
    private final Map<BytesRef, GroupHead> groups;

    private Scorer scorer;

    GeneralAllGroupHeadsCollector(String groupField, Sort sortWithinGroup) {
      super(groupField, sortWithinGroup.getSort().length);
      this.sortWithinGroup = sortWithinGroup;
      groups = new HashMap<BytesRef, GroupHead>();

      final SortField[] sortFields = sortWithinGroup.getSort();
      for (int i = 0; i < sortFields.length; i++) {
        reversed[i] = sortFields[i].getReverse() ? -1 : 1;
      }
    }

    @Override
    protected void retrieveGroupHeadAndAddIfNotExist(int doc) throws IOException {
      final int ord = groupIndex.getOrd(doc);
      final BytesRef groupValue;
      if (ord == -1) {
        groupValue = null;
      } else {
        groupIndex.lookupOrd(ord, scratchBytesRef);
        groupValue = scratchBytesRef;
      }
      GroupHead groupHead = groups.get(groupValue);
      if (groupHead == null) {
        groupHead = new GroupHead(groupValue, sortWithinGroup, doc);
        groups.put(groupValue == null ? null : BytesRef.deepCopyOf(groupValue), groupHead);
        temporalResult.stop = true;
      } else {
        temporalResult.stop = false;
      }
      temporalResult.groupHead = groupHead;
    }

    @Override
    protected Collection<GroupHead> getCollectedGroupHeads() {
      return groups.values();
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
      this.readerContext = context;
      groupIndex = FieldCache.DEFAULT.getTermsIndex(context.reader(), groupField);

      for (GroupHead groupHead : groups.values()) {
        for (int i = 0; i < groupHead.comparators.length; i++) {
          groupHead.comparators[i] = groupHead.comparators[i].setNextReader(context);
        }
      }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
      for (GroupHead groupHead : groups.values()) {
        for (FieldComparator<?> comparator : groupHead.comparators) {
          comparator.setScorer(scorer);
        }
      }
    }

    class GroupHead extends AbstractAllGroupHeadsCollector.GroupHead<BytesRef> {

      final FieldComparator<?>[] comparators;

      @SuppressWarnings({"unchecked","rawtypes"})
      private GroupHead(BytesRef groupValue, Sort sort, int doc) throws IOException {
        super(groupValue, doc + readerContext.docBase);
        final SortField[] sortFields = sort.getSort();
        comparators = new FieldComparator[sortFields.length];
        for (int i = 0; i < sortFields.length; i++) {
          comparators[i] = sortFields[i].getComparator(1, i).setNextReader(readerContext);
          comparators[i].setScorer(scorer);
          comparators[i].copy(0, doc);
          comparators[i].setBottom(0);
        }
      }

      @Override
      public int compare(int compIDX, int doc) throws IOException {
        return comparators[compIDX].compareBottom(doc);
      }

      @Override
      public void updateDocHead(int doc) throws IOException {
        for (FieldComparator<?> comparator : comparators) {
          comparator.copy(0, doc);
          comparator.setBottom(0);
        }
        this.doc = doc + readerContext.docBase;
      }
    }
  }


  // AbstractAllGroupHeadsCollector optimized for ord fields and scores.
  static class OrdScoreAllGroupHeadsCollector extends TermAllGroupHeadsCollector<OrdScoreAllGroupHeadsCollector.GroupHead> {

    private final SentinelIntSet ordSet;
    private final List<GroupHead> collectedGroups;
    private final SortField[] fields;

    private SortedDocValues[] sortsIndex;
    private Scorer scorer;
    private GroupHead[] segmentGroupHeads;

    OrdScoreAllGroupHeadsCollector(String groupField, Sort sortWithinGroup, int initialSize) {
      super(groupField, sortWithinGroup.getSort().length);
      ordSet = new SentinelIntSet(initialSize, -2);
      collectedGroups = new ArrayList<GroupHead>(initialSize);

      final SortField[] sortFields = sortWithinGroup.getSort();
      fields = new SortField[sortFields.length];
      sortsIndex = new SortedDocValues[sortFields.length];
      for (int i = 0; i < sortFields.length; i++) {
        reversed[i] = sortFields[i].getReverse() ? -1 : 1;
        fields[i] = sortFields[i];
      }
    }

    @Override
    protected Collection<GroupHead> getCollectedGroupHeads() {
      return collectedGroups;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
    }

    @Override
    protected void retrieveGroupHeadAndAddIfNotExist(int doc) throws IOException {
      int key = groupIndex.getOrd(doc);
      GroupHead groupHead;
      if (!ordSet.exists(key)) {
        ordSet.put(key);
        BytesRef term;
        if (key == -1) {
          term = null;
        } else {
          term = new BytesRef();
          groupIndex.lookupOrd(key, term);
        }
        groupHead = new GroupHead(doc, term);
        collectedGroups.add(groupHead);
        segmentGroupHeads[key+1] = groupHead;
        temporalResult.stop = true;
      } else {
        temporalResult.stop = false;
        groupHead = segmentGroupHeads[key+1];
      }
      temporalResult.groupHead = groupHead;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
      this.readerContext = context;
      groupIndex = FieldCache.DEFAULT.getTermsIndex(context.reader(), groupField);
      for (int i = 0; i < fields.length; i++) {
        if (fields[i].getType() == SortField.Type.SCORE) {
          continue;
        }

        sortsIndex[i] = FieldCache.DEFAULT.getTermsIndex(context.reader(), fields[i].getField());
      }

      // Clear ordSet and fill it with previous encountered groups that can occur in the current segment.
      ordSet.clear();
      segmentGroupHeads = new GroupHead[groupIndex.getValueCount()+1];
      for (GroupHead collectedGroup : collectedGroups) {
        int ord;
        if (collectedGroup.groupValue == null) {
          ord = -1;
        } else {
          ord = groupIndex.lookupTerm(collectedGroup.groupValue);
        }
        if (collectedGroup.groupValue == null || ord >= 0) {
          ordSet.put(ord);
          segmentGroupHeads[ord+1] = collectedGroup;

          for (int i = 0; i < sortsIndex.length; i++) {
            if (fields[i].getType() == SortField.Type.SCORE) {
              continue;
            }
            int sortOrd;
            if (collectedGroup.sortValues[i] == null) {
              sortOrd = -1;
            } else {
              sortOrd = sortsIndex[i].lookupTerm(collectedGroup.sortValues[i]);
            }
            collectedGroup.sortOrds[i] = sortOrd;
          }
        }
      }
    }

    class GroupHead extends AbstractAllGroupHeadsCollector.GroupHead<BytesRef> {

      BytesRef[] sortValues;
      int[] sortOrds;
      float[] scores;

      private GroupHead(int doc, BytesRef groupValue) throws IOException {
        super(groupValue, doc + readerContext.docBase);
        sortValues = new BytesRef[sortsIndex.length];
        sortOrds = new int[sortsIndex.length];
        scores = new float[sortsIndex.length];
        for (int i = 0; i < sortsIndex.length; i++) {
          if (fields[i].getType() == SortField.Type.SCORE) {
            scores[i] = scorer.score();
          } else {
            sortOrds[i] = sortsIndex[i].getOrd(doc);
            sortValues[i] = new BytesRef();
            if (sortOrds[i] != -1) {
              sortsIndex[i].get(doc, sortValues[i]);
            }
          }
        }
      }

      @Override
      public int compare(int compIDX, int doc) throws IOException {
        if (fields[compIDX].getType() == SortField.Type.SCORE) {
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
            if (sortsIndex[compIDX].getOrd(doc) == -1) {
              scratchBytesRef.length = 0;
            } else {
              sortsIndex[compIDX].get(doc, scratchBytesRef);
            }
            return sortValues[compIDX].compareTo(scratchBytesRef);
          } else {
            return sortOrds[compIDX] - sortsIndex[compIDX].getOrd(doc);
          }
        }
      }

      @Override
      public void updateDocHead(int doc) throws IOException {
        for (int i = 0; i < sortsIndex.length; i++) {
          if (fields[i].getType() == SortField.Type.SCORE) {
            scores[i] = scorer.score();
          } else {
            sortOrds[i] = sortsIndex[i].getOrd(doc);
            if (sortOrds[i] == -1) {
              sortValues[i].length = 0;
            } else {
              sortsIndex[i].get(doc, sortValues[i]);
            }
          }
        }
        this.doc = doc + readerContext.docBase;
      }
    }
  }


  // AbstractAllGroupHeadsCollector optimized for ord fields.
  static class OrdAllGroupHeadsCollector extends TermAllGroupHeadsCollector<OrdAllGroupHeadsCollector.GroupHead> {

    private final SentinelIntSet ordSet;
    private final List<GroupHead> collectedGroups;
    private final SortField[] fields;

    private SortedDocValues[] sortsIndex;
    private GroupHead[] segmentGroupHeads;

    OrdAllGroupHeadsCollector(String groupField, Sort sortWithinGroup, int initialSize) {
      super(groupField, sortWithinGroup.getSort().length);
      ordSet = new SentinelIntSet(initialSize, -2);
      collectedGroups = new ArrayList<GroupHead>(initialSize);

      final SortField[] sortFields = sortWithinGroup.getSort();
      fields = new SortField[sortFields.length];
      sortsIndex = new SortedDocValues[sortFields.length];
      for (int i = 0; i < sortFields.length; i++) {
        reversed[i] = sortFields[i].getReverse() ? -1 : 1;
        fields[i] = sortFields[i];
      }
    }

    @Override
    protected Collection<GroupHead> getCollectedGroupHeads() {
      return collectedGroups;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    @Override
    protected void retrieveGroupHeadAndAddIfNotExist(int doc) throws IOException {
      int key = groupIndex.getOrd(doc);
      GroupHead groupHead;
      if (!ordSet.exists(key)) {
        ordSet.put(key);
        BytesRef term;
        if (key == -1) {
          term = null;
        } else {
          term = new BytesRef();
          groupIndex.lookupOrd(key, term);
        }
        groupHead = new GroupHead(doc, term);
        collectedGroups.add(groupHead);
        segmentGroupHeads[key+1] = groupHead;
        temporalResult.stop = true;
      } else {
        temporalResult.stop = false;
        groupHead = segmentGroupHeads[key+1];
      }
      temporalResult.groupHead = groupHead;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
      this.readerContext = context;
      groupIndex = FieldCache.DEFAULT.getTermsIndex(context.reader(), groupField);
      for (int i = 0; i < fields.length; i++) {
        sortsIndex[i] = FieldCache.DEFAULT.getTermsIndex(context.reader(), fields[i].getField());
      }

      // Clear ordSet and fill it with previous encountered groups that can occur in the current segment.
      ordSet.clear();
      segmentGroupHeads = new GroupHead[groupIndex.getValueCount()+1];
      for (GroupHead collectedGroup : collectedGroups) {
        int groupOrd;
        if (collectedGroup.groupValue == null) {
          groupOrd = -1;
        } else {
          groupOrd = groupIndex.lookupTerm(collectedGroup.groupValue);
        }
        if (collectedGroup.groupValue == null || groupOrd >= 0) {
          ordSet.put(groupOrd);
          segmentGroupHeads[groupOrd+1] = collectedGroup;

          for (int i = 0; i < sortsIndex.length; i++) {
            int sortOrd;
            if (collectedGroup.sortOrds[i] == -1) {
              sortOrd = -1;
            } else {
              sortOrd = sortsIndex[i].lookupTerm(collectedGroup.sortValues[i]);
            }
            collectedGroup.sortOrds[i] = sortOrd;
          }
        }
      }
    }

    class GroupHead extends AbstractAllGroupHeadsCollector.GroupHead<BytesRef> {

      BytesRef[] sortValues;
      int[] sortOrds;

      private GroupHead(int doc, BytesRef groupValue) {
        super(groupValue, doc + readerContext.docBase);
        sortValues = new BytesRef[sortsIndex.length];
        sortOrds = new int[sortsIndex.length];
        for (int i = 0; i < sortsIndex.length; i++) {
          sortOrds[i] = sortsIndex[i].getOrd(doc);
          sortValues[i] = new BytesRef();
          if (sortOrds[i] != -1) {
            sortsIndex[i].get(doc, sortValues[i]);
          }
        }
      }

      @Override
      public int compare(int compIDX, int doc) throws IOException {
        if (sortOrds[compIDX] < 0) {
          // The current segment doesn't contain the sort value we encountered before. Therefore the ord is negative.
          if (sortsIndex[compIDX].getOrd(doc) == -1) {
            scratchBytesRef.length = 0;
          } else {
            sortsIndex[compIDX].get(doc, scratchBytesRef);
          }
          return sortValues[compIDX].compareTo(scratchBytesRef);
        } else {
          return sortOrds[compIDX] - sortsIndex[compIDX].getOrd(doc);
        }
      }

      @Override
      public void updateDocHead(int doc) throws IOException {
        for (int i = 0; i < sortsIndex.length; i++) {
          sortOrds[i] = sortsIndex[i].getOrd(doc);
          if (sortOrds[i] == -1) {
            sortValues[i].length = 0;
          } else {
            sortsIndex[i].lookupOrd(sortOrds[i], sortValues[i]);
          }
        }
        this.doc = doc + readerContext.docBase;
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
      ordSet = new SentinelIntSet(initialSize, -2);
      collectedGroups = new ArrayList<GroupHead>(initialSize);

      final SortField[] sortFields = sortWithinGroup.getSort();
      fields = new SortField[sortFields.length];
      for (int i = 0; i < sortFields.length; i++) {
        reversed[i] = sortFields[i].getReverse() ? -1 : 1;
        fields[i] = sortFields[i];
      }
    }

    @Override
    protected Collection<GroupHead> getCollectedGroupHeads() {
      return collectedGroups;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
    }

    @Override
    protected void retrieveGroupHeadAndAddIfNotExist(int doc) throws IOException {
      int key = groupIndex.getOrd(doc);
      GroupHead groupHead;
      if (!ordSet.exists(key)) {
        ordSet.put(key);
        BytesRef term;
        if (key == -1) {
          term = null;
        } else {
          term = new BytesRef();
          groupIndex.lookupOrd(key, term);
        }
        groupHead = new GroupHead(doc, term);
        collectedGroups.add(groupHead);
        segmentGroupHeads[key+1] = groupHead;
        temporalResult.stop = true;
      } else {
        temporalResult.stop = false;
        groupHead = segmentGroupHeads[key+1];
      }
      temporalResult.groupHead = groupHead;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
      this.readerContext = context;
      groupIndex = FieldCache.DEFAULT.getTermsIndex(context.reader(), groupField);

      // Clear ordSet and fill it with previous encountered groups that can occur in the current segment.
      ordSet.clear();
      segmentGroupHeads = new GroupHead[groupIndex.getValueCount()+1];
      for (GroupHead collectedGroup : collectedGroups) {
        int ord;
        if (collectedGroup.groupValue == null) {
          ord = -1;
        } else {
          ord = groupIndex.lookupTerm(collectedGroup.groupValue);
        }
        if (collectedGroup.groupValue == null || ord >= 0) {
          ordSet.put(ord);
          segmentGroupHeads[ord+1] = collectedGroup;
        }
      }
    }

    class GroupHead extends AbstractAllGroupHeadsCollector.GroupHead<BytesRef> {

      float[] scores;

      private GroupHead(int doc, BytesRef groupValue) throws IOException {
        super(groupValue, doc + readerContext.docBase);
        scores = new float[fields.length];
        float score = scorer.score();
        for (int i = 0; i < scores.length; i++) {
          scores[i] = score;
        }
      }

      @Override
      public int compare(int compIDX, int doc) throws IOException {
        float score = scorer.score();
        if (scores[compIDX] < score) {
          return 1;
        } else if (scores[compIDX] > score) {
          return -1;
        }
        return 0;
      }

      @Override
      public void updateDocHead(int doc) throws IOException {
        float score = scorer.score();
        for (int i = 0; i < scores.length; i++) {
          scores[i] = score;
        }
        this.doc = doc + readerContext.docBase;
      }

    }

  }
}
