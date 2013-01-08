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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.AbstractSecondPassGroupingCollector;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SentinelIntSet;

import java.io.IOException;
import java.util.Collection;
import java.util.Locale;

/**
 * IDV based implementation of {@link AbstractSecondPassGroupingCollector}.
 *
 * @lucene.experimental
 */
public abstract class DVSecondPassGroupingCollector<GROUP_VALUE> extends AbstractSecondPassGroupingCollector<GROUP_VALUE> {

  /**
   * Constructs a {@link DVSecondPassGroupingCollector}.
   * Selects and constructs the most optimal second pass collector implementation for grouping by {@link DocValues}.
   *
   * @param groupField      The field to group by
   * @param diskResident    Whether the values to group by should be disk resident
   * @param type            The {@link Type} which is used to select a concrete implementation.
   * @param searchGroups    The groups from the first phase search
   * @param groupSort       The sort used for the groups
   * @param withinGroupSort The sort used for documents inside a group
   * @param maxDocsPerGroup The maximum number of documents to collect per group
   * @param getScores       Whether to include scores for the documents inside a group
   * @param getMaxScores    Whether to keep track of the higest score per group
   * @param fillSortFields  Whether to include the sort values
   * @return the most optimal second pass collector implementation for grouping by {@link DocValues}
   * @throws IOException    If I/O related errors occur
   */
  @SuppressWarnings("unchecked")
  public static <T> DVSecondPassGroupingCollector<T> create(String groupField,
                                                     boolean diskResident,
                                                     DocValues.Type type,
                                                     Collection<SearchGroup<T>> searchGroups,
                                                     Sort groupSort,
                                                     Sort withinGroupSort,
                                                     int maxDocsPerGroup,
                                                     boolean getScores,
                                                     boolean getMaxScores,
                                                     boolean fillSortFields) throws IOException {
    switch (type) {
      case VAR_INTS:
      case FIXED_INTS_8:
      case FIXED_INTS_16:
      case FIXED_INTS_32:
      case FIXED_INTS_64:
        // Type erasure b/c otherwise we have inconvertible types...
        return (DVSecondPassGroupingCollector) new Lng(groupField, type, diskResident, (Collection) searchGroups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields);
      case FLOAT_32:
      case FLOAT_64:
        // Type erasure b/c otherwise we have inconvertible types...
        return (DVSecondPassGroupingCollector) new Dbl(groupField, type, diskResident, (Collection) searchGroups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields);
      case BYTES_FIXED_STRAIGHT:
      case BYTES_FIXED_DEREF:
      case BYTES_VAR_STRAIGHT:
      case BYTES_VAR_DEREF:
        // Type erasure b/c otherwise we have inconvertible types...
        return (DVSecondPassGroupingCollector) new BR(groupField, type, diskResident, (Collection) searchGroups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields);
      case BYTES_VAR_SORTED:
      case BYTES_FIXED_SORTED:
        // Type erasure b/c otherwise we have inconvertible types...
        return (DVSecondPassGroupingCollector) new SortedBR(groupField, type, diskResident, (Collection) searchGroups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields);
      default:
        throw new IllegalArgumentException(String.format(Locale.ROOT, "ValueType %s not supported", type));
    }
  }

  final String groupField;
  final DocValues.Type valueType;
  final boolean diskResident;

  DVSecondPassGroupingCollector(String groupField, DocValues.Type valueType, boolean diskResident, Collection<SearchGroup<GROUP_VALUE>> searchGroups, Sort groupSort, Sort withinGroupSort, int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields) throws IOException {
    super(searchGroups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields);
    this.groupField = groupField;
    this.valueType = valueType;
    this.diskResident = diskResident;
  }

  @Override
  public void setNextReader(AtomicReaderContext readerContext) throws IOException {
    super.setNextReader(readerContext);

    final DocValues dv = readerContext.reader().docValues(groupField);
    final DocValues.Source dvSource;
    if (dv != null) {
      dvSource = diskResident ? dv.getDirectSource() : dv.getSource();
    } else {
      dvSource = getDefaultSource(readerContext);
    }
    setDocValuesSources(dvSource, readerContext);
  }

  /**
   * Sets the idv source for concrete implementations to use.
   *
   * @param source The idv source to be used by concrete implementations
   * @param readerContext The current reader context
   */
  protected abstract void setDocValuesSources(DocValues.Source source, AtomicReaderContext readerContext);

  /**
   * @return The default source when no doc values are available.
   * @param readerContext The current reader context
   */
  protected DocValues.Source getDefaultSource(AtomicReaderContext readerContext) {
    return DocValues.getDefaultSource(valueType);
  }

  static class Lng extends DVSecondPassGroupingCollector<Long> {

    private DocValues.Source source;

    Lng(String groupField, DocValues.Type valueType, boolean diskResident, Collection<SearchGroup<Long>> searchGroups, Sort groupSort, Sort withinGroupSort, int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields) throws IOException {
      super(groupField, valueType, diskResident, searchGroups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields);
    }

    @Override
    protected SearchGroupDocs<Long> retrieveGroup(int doc) throws IOException {
      return groupMap.get(source.getInt(doc));
    }

    @Override
    protected void setDocValuesSources(DocValues.Source source, AtomicReaderContext readerContext) {
      this.source = source;
    }
  }

  static class Dbl extends DVSecondPassGroupingCollector<Double> {

    private DocValues.Source source;

    Dbl(String groupField, DocValues.Type valueType, boolean diskResident, Collection<SearchGroup<Double>> searchGroups, Sort groupSort, Sort withinGroupSort, int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields) throws IOException {
      super(groupField, valueType, diskResident, searchGroups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields);
    }

    @Override
    protected SearchGroupDocs<Double> retrieveGroup(int doc) throws IOException {
      return groupMap.get(source.getFloat(doc));
    }

    @Override
    protected void setDocValuesSources(DocValues.Source source, AtomicReaderContext readerContext) {
      this.source = source;
    }
  }

  static class BR extends DVSecondPassGroupingCollector<BytesRef> {

    private DocValues.Source source;
    private final BytesRef spare = new BytesRef();

    BR(String groupField, DocValues.Type valueType, boolean diskResident, Collection<SearchGroup<BytesRef>> searchGroups, Sort groupSort, Sort withinGroupSort, int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields) throws IOException {
      super(groupField, valueType, diskResident, searchGroups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields);
    }

    @Override
    protected SearchGroupDocs<BytesRef> retrieveGroup(int doc) throws IOException {
      return groupMap.get(source.getBytes(doc, spare));
    }

    @Override
    protected void setDocValuesSources(DocValues.Source source, AtomicReaderContext readerContext) {
      this.source = source;
    }

  }

  static class SortedBR extends DVSecondPassGroupingCollector<BytesRef> {

    private DocValues.SortedSource source;
    private final BytesRef spare = new BytesRef();
    private final SentinelIntSet ordSet;

    @SuppressWarnings({"unchecked","rawtypes"})
    SortedBR(String groupField,  DocValues.Type valueType, boolean diskResident, Collection<SearchGroup<BytesRef>> searchGroups, Sort groupSort, Sort withinGroupSort, int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields) throws IOException {
      super(groupField, valueType, diskResident, searchGroups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields);
      ordSet = new SentinelIntSet(groupMap.size(), -1);
      groupDocs = (SearchGroupDocs<BytesRef>[]) new SearchGroupDocs[ordSet.keys.length];
    }

    @Override
    protected SearchGroupDocs<BytesRef> retrieveGroup(int doc) throws IOException {
      int slot = ordSet.find(source.ord(doc));
      if (slot >= 0) {
        return groupDocs[slot];
      }

      return null;
    }

    @Override
    protected void setDocValuesSources(DocValues.Source source, AtomicReaderContext readerContext) {
      this.source = source.asSortedSource();

      ordSet.clear();
      for (SearchGroupDocs<BytesRef> group : groupMap.values()) {
        int ord = this.source.getOrdByValue(group.groupValue, spare);
        if (ord >= 0) {
          groupDocs[ordSet.put(ord)] = group;
        }
      }
    }

    @Override
    protected DocValues.Source getDefaultSource(AtomicReaderContext readerContext) {
      return DocValues.getDefaultSortedSource(valueType, readerContext.reader().maxDoc());
    }
  }

}
