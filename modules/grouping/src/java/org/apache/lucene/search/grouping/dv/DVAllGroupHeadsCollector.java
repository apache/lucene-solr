package org.apache.lucene.search.grouping.dv;

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
import org.apache.lucene.index.values.IndexDocValues;
import org.apache.lucene.index.values.ValueType;
import org.apache.lucene.search.*;
import org.apache.lucene.search.grouping.AbstractAllGroupHeadsCollector;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A base implementation of {@link org.apache.lucene.search.grouping.AbstractAllGroupHeadsCollector} for retrieving
 * the most relevant groups when grouping on a indexed doc values field.
 *
 * @lucene.experimental
 */
//TODO - (MvG): Add more optimized implementations
public abstract class DVAllGroupHeadsCollector<GH extends AbstractAllGroupHeadsCollector.GroupHead> extends AbstractAllGroupHeadsCollector<GH> {

  final String groupField;
  final boolean diskResident;
  final ValueType valueType;
  final BytesRef scratchBytesRef = new BytesRef();

  IndexReader.AtomicReaderContext readerContext;
  Scorer scorer;

  DVAllGroupHeadsCollector(String groupField, ValueType valueType, int numberOfSorts, boolean diskResident) {
    super(numberOfSorts);
    this.groupField = groupField;
    this.valueType = valueType;
    this.diskResident = diskResident;
  }

  /**
   * Creates an <code>AbstractAllGroupHeadsCollector</code> instance based on the supplied arguments.
   * This factory method decides with implementation is best suited.
   *
   * @param groupField      The field to group by
   * @param sortWithinGroup The sort within each group
   * @param type The {@link ValueType} which is used to select a concrete implementation.
   * @param diskResident Whether the values to group by should be disk resident
   * @return an <code>AbstractAllGroupHeadsCollector</code> instance based on the supplied arguments
   * @throws IOException If I/O related errors occur
   */
  public static AbstractAllGroupHeadsCollector create(String groupField, Sort sortWithinGroup, ValueType type, boolean diskResident) throws IOException {
    switch (type) {
      case VAR_INTS:
      case FIXED_INTS_8:
      case FIXED_INTS_16:
      case FIXED_INTS_32:
      case FIXED_INTS_64:
        return new GeneralAllGroupHeadsCollector.Lng(groupField, type, sortWithinGroup, diskResident);
      case FLOAT_32:
      case FLOAT_64:
        return new GeneralAllGroupHeadsCollector.Dbl(groupField, type, sortWithinGroup, diskResident);
      case BYTES_FIXED_STRAIGHT:
      case BYTES_FIXED_DEREF:
      case BYTES_VAR_STRAIGHT:
      case BYTES_VAR_DEREF:
        return new GeneralAllGroupHeadsCollector.BR(groupField, type, sortWithinGroup, diskResident);
      case BYTES_VAR_SORTED:
      case BYTES_FIXED_SORTED:
        return new GeneralAllGroupHeadsCollector.SortedBR(groupField, type, sortWithinGroup, diskResident);
      default:
        throw new IllegalArgumentException(String.format("ValueType %s not supported", type));
    }
  }

  static class GroupHead extends AbstractAllGroupHeadsCollector.GroupHead<Comparable> {

    final FieldComparator[] comparators;
    IndexReader.AtomicReaderContext readerContext;
    Scorer scorer;

    GroupHead(Comparable groupValue, Sort sort, int doc, IndexReader.AtomicReaderContext readerContext, Scorer scorer) throws IOException {
      super(groupValue, doc + readerContext.docBase);
      final SortField[] sortFields = sort.getSort();
      comparators = new FieldComparator[sortFields.length];
      for (int i = 0; i < sortFields.length; i++) {
        comparators[i] = sortFields[i].getComparator(1, i).setNextReader(readerContext);
        comparators[i].setScorer(scorer);
        comparators[i].copy(0, doc);
        comparators[i].setBottom(0);
      }

      this.readerContext = readerContext;
      this.scorer = scorer;
    }

    public int compare(int compIDX, int doc) throws IOException {
      return comparators[compIDX].compareBottom(doc);
    }

    public void updateDocHead(int doc) throws IOException {
      for (FieldComparator comparator : comparators) {
        comparator.copy(0, doc);
        comparator.setBottom(0);
      }
      this.doc = doc + readerContext.docBase;
    }
  }

  @Override
  public void setNextReader(IndexReader.AtomicReaderContext readerContext) throws IOException {
    this.readerContext = readerContext;

    final IndexDocValues dv = readerContext.reader.docValues(groupField);
    final IndexDocValues.Source dvSource;
    if (dv != null) {
      dvSource = diskResident ? dv.getDirectSource() : dv.getSource();
    } else {
      dvSource = getDefaultSource(readerContext);
    }
    setDocValuesSources(dvSource);
  }

  /**
   * Sets the idv source for concrete implementations to use.
   *
   * @param source The idv source to be used by concrete implementations
   */
  protected abstract void setDocValuesSources(IndexDocValues.Source source);

  /**
   * @return The default source when no doc values are available.
   * @param readerContext The current reader context
   */
  protected IndexDocValues.Source getDefaultSource(IndexReader.AtomicReaderContext readerContext) {
    return IndexDocValues.getDefaultSource(valueType);
  }

  // A general impl that works for any group sort.
  static abstract class GeneralAllGroupHeadsCollector extends DVAllGroupHeadsCollector<DVAllGroupHeadsCollector.GroupHead> {

    private final Sort sortWithinGroup;
    private final Map<Comparable, GroupHead> groups;

    GeneralAllGroupHeadsCollector(String groupField, ValueType valueType, Sort sortWithinGroup, boolean diskResident) throws IOException {
      super(groupField, valueType, sortWithinGroup.getSort().length, diskResident);
      this.sortWithinGroup = sortWithinGroup;
      groups = new HashMap<Comparable, GroupHead>();

      final SortField[] sortFields = sortWithinGroup.getSort();
      for (int i = 0; i < sortFields.length; i++) {
        reversed[i] = sortFields[i].getReverse() ? -1 : 1;
      }
    }

    protected void retrieveGroupHeadAndAddIfNotExist(int doc) throws IOException {
      final Comparable groupValue = getGroupValue(doc);
      GroupHead groupHead = groups.get(groupValue);
      if (groupHead == null) {
        groupHead = new GroupHead(groupValue, sortWithinGroup, doc, readerContext, scorer);
        groups.put(groupValue == null ? null : duplicate(groupValue), groupHead);
        temporalResult.stop = true;
      } else {
        temporalResult.stop = false;
      }
      temporalResult.groupHead = groupHead;
    }

    protected abstract Comparable getGroupValue(int doc);

    protected abstract Comparable duplicate(Comparable value);

    protected Collection<GroupHead> getCollectedGroupHeads() {
      return groups.values();
    }

    public void setNextReader(IndexReader.AtomicReaderContext context) throws IOException {
      super.setNextReader(context);
      for (GroupHead groupHead : groups.values()) {
        for (int i = 0; i < groupHead.comparators.length; i++) {
          groupHead.comparators[i] = groupHead.comparators[i].setNextReader(context);
          groupHead.readerContext = context;
        }
      }
    }

    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
      for (GroupHead groupHead : groups.values()) {
        groupHead.scorer = scorer;
        for (FieldComparator comparator : groupHead.comparators) {
          comparator.setScorer(scorer);
        }
      }
    }

    static class SortedBR extends GeneralAllGroupHeadsCollector {

      private IndexDocValues.SortedSource source;

      SortedBR(String groupField, ValueType valueType, Sort sortWithinGroup, boolean diskResident) throws IOException {
        super(groupField, valueType, sortWithinGroup, diskResident);
      }

      protected Comparable getGroupValue(int doc) {
        return source.getBytes(doc, scratchBytesRef);
      }

      protected Comparable duplicate(Comparable value) {
        return BytesRef.deepCopyOf((BytesRef) value);
      }

      protected void setDocValuesSources(IndexDocValues.Source source) {
        this.source = source.asSortedSource();
      }

      @Override
      protected IndexDocValues.Source getDefaultSource(IndexReader.AtomicReaderContext readerContext) {
        return IndexDocValues.getDefaultSortedSource(valueType, readerContext.reader.maxDoc());
      }
    }

    static class BR extends GeneralAllGroupHeadsCollector {

      private IndexDocValues.Source source;

      BR(String groupField, ValueType valueType, Sort sortWithinGroup, boolean diskResident) throws IOException {
        super(groupField, valueType, sortWithinGroup, diskResident);
      }

      protected Comparable getGroupValue(int doc) {
        return source.getBytes(doc, scratchBytesRef);
      }

      protected Comparable duplicate(Comparable value) {
        return BytesRef.deepCopyOf((BytesRef) value);
      }

      protected void setDocValuesSources(IndexDocValues.Source source) {
        this.source = source;
      }

    }

    static class Lng extends GeneralAllGroupHeadsCollector {

      private IndexDocValues.Source source;

      Lng(String groupField, ValueType valueType, Sort sortWithinGroup, boolean diskResident) throws IOException {
        super(groupField, valueType, sortWithinGroup, diskResident);
      }

      protected Comparable getGroupValue(int doc) {
        return source.getInt(doc);
      }

      protected Comparable duplicate(Comparable value) {
        return value;
      }

      protected void setDocValuesSources(IndexDocValues.Source source) {
        this.source = source;
      }
    }

    static class Dbl extends GeneralAllGroupHeadsCollector {

      private IndexDocValues.Source source;

      Dbl(String groupField, ValueType valueType, Sort sortWithinGroup, boolean diskResident) throws IOException {
        super(groupField, valueType, sortWithinGroup, diskResident);
      }

      protected Comparable getGroupValue(int doc) {
        return source.getFloat(doc);
      }

      protected Comparable duplicate(Comparable value) {
        return value;
      }

      protected void setDocValuesSources(IndexDocValues.Source source) {
        this.source = source;
      }

    }

  }

}
