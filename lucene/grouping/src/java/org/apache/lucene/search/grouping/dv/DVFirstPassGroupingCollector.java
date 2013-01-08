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
import org.apache.lucene.index.DocValues.Type; // javadocs
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.AbstractFirstPassGroupingCollector;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Locale;

/**
 * IDV based Implementations of {@link AbstractFirstPassGroupingCollector}.
 *
 * @lucene.experimental 
 */
public abstract class DVFirstPassGroupingCollector<GROUP_VALUE_TYPE> extends AbstractFirstPassGroupingCollector<GROUP_VALUE_TYPE> {

  final String groupField;
  final boolean diskResident;
  final DocValues.Type valueType;

  /**
   * Constructs a {@link DVFirstPassGroupingCollector}.
   * Selects and constructs the most optimal first pass collector implementation for grouping by {@link DocValues}.
   *
   * @param groupField      The field to group by
   * @param topNGroups      The maximum top number of groups to return. Typically this equals to offset + rows.
   * @param diskResident    Whether the values to group by should be disk resident
   * @param type            The {@link Type} which is used to select a concrete implementation.
   * @param groupSort       The sort used for the groups
   * @return the most optimal first pass collector implementation for grouping by {@link DocValues}
   * @throws IOException    If I/O related errors occur
   */
  @SuppressWarnings("unchecked")
  public static <T> DVFirstPassGroupingCollector<T> create(Sort groupSort, int topNGroups, String groupField, DocValues.Type type, boolean diskResident) throws IOException {
    switch (type) {
      case VAR_INTS:
      case FIXED_INTS_8:
      case FIXED_INTS_16:
      case FIXED_INTS_32:
      case FIXED_INTS_64:
        // Type erasure b/c otherwise we have inconvertible types...
        return (DVFirstPassGroupingCollector) new Lng(groupSort, topNGroups, groupField, diskResident, type);
      case FLOAT_32:
      case FLOAT_64:
        // Type erasure b/c otherwise we have inconvertible types...
        return (DVFirstPassGroupingCollector) new Dbl(groupSort, topNGroups, groupField, diskResident, type);
      case BYTES_FIXED_STRAIGHT:
      case BYTES_FIXED_DEREF:
      case BYTES_VAR_STRAIGHT:
      case BYTES_VAR_DEREF:
        // Type erasure b/c otherwise we have inconvertible types...
        return (DVFirstPassGroupingCollector) new BR(groupSort, topNGroups, groupField, diskResident, type);
      case BYTES_VAR_SORTED:
      case BYTES_FIXED_SORTED:
        // Type erasure b/c otherwise we have inconvertible types...
        return (DVFirstPassGroupingCollector) new SortedBR(groupSort, topNGroups, groupField, diskResident, type);
      default:
        throw new IllegalArgumentException(String.format(Locale.ROOT, "ValueType %s not supported", type));
    }
  }

  DVFirstPassGroupingCollector(Sort groupSort, int topNGroups, String groupField, boolean diskResident, DocValues.Type valueType) throws IOException {
    super(groupSort, topNGroups);
    this.groupField = groupField;
    this.diskResident = diskResident;
    this.valueType = valueType;
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
    setDocValuesSources(dvSource);
  }

  /**
   * Sets the idv source for concrete implementations to use.
   *
   * @param source The idv source to be used by concrete implementations
   */
  protected abstract void setDocValuesSources(DocValues.Source source);

  /**
   * @return The default source when no doc values are available.
   * @param readerContext The current reader context
   */
  protected DocValues.Source getDefaultSource(AtomicReaderContext readerContext) {
    return DocValues.getDefaultSource(valueType);
  }

  static class Lng extends DVFirstPassGroupingCollector<Long> {

    private DocValues.Source source;

    Lng(Sort groupSort, int topNGroups, String groupField, boolean diskResident, DocValues.Type type) throws IOException {
      super(groupSort, topNGroups, groupField, diskResident, type);
    }

    @Override
    protected Long getDocGroupValue(int doc) {
      return source.getInt(doc);
    }

    @Override
    protected Long copyDocGroupValue(Long groupValue, Long reuse) {
      return groupValue;
    }

    @Override
    protected void setDocValuesSources(DocValues.Source source) {
      this.source = source;
    }
  }

  static class Dbl extends DVFirstPassGroupingCollector<Double> {

    private DocValues.Source source;

    Dbl(Sort groupSort, int topNGroups, String groupField, boolean diskResident, DocValues.Type type) throws IOException {
      super(groupSort, topNGroups, groupField, diskResident, type);
    }

    @Override
    protected Double getDocGroupValue(int doc) {
      return source.getFloat(doc);
    }

    @Override
    protected Double copyDocGroupValue(Double groupValue, Double reuse) {
      return groupValue;
    }

    @Override
    protected void setDocValuesSources(DocValues.Source source) {
      this.source = source;
    }
  }

  static class BR extends DVFirstPassGroupingCollector<BytesRef> {

    private DocValues.Source source;
    private final BytesRef spare = new BytesRef();

    BR(Sort groupSort, int topNGroups, String groupField, boolean diskResident, DocValues.Type type) throws IOException {
      super(groupSort, topNGroups, groupField, diskResident, type);
    }

    @Override
    protected BytesRef getDocGroupValue(int doc) {
      return source.getBytes(doc, spare);
    }

    @Override
    protected BytesRef copyDocGroupValue(BytesRef groupValue, BytesRef reuse) {
      if (reuse != null) {
        reuse.copyBytes(groupValue);
        return reuse;
      } else {
        return BytesRef.deepCopyOf(groupValue);
      }
    }

    @Override
    protected void setDocValuesSources(DocValues.Source source) {
      this.source = source;
    }
  }

  static class SortedBR extends DVFirstPassGroupingCollector<BytesRef> {

    private DocValues.SortedSource sortedSource;
    private final BytesRef spare = new BytesRef();

    SortedBR(Sort groupSort, int topNGroups, String groupField, boolean diskResident, DocValues.Type type) throws IOException {
      super(groupSort, topNGroups, groupField, diskResident, type);
    }

    @Override
    protected BytesRef getDocGroupValue(int doc) {
      return sortedSource.getBytes(doc, spare);
    }

    @Override
    protected BytesRef copyDocGroupValue(BytesRef groupValue, BytesRef reuse) {
      if (reuse != null) {
        reuse.copyBytes(groupValue);
        return reuse;
      } else {
        return BytesRef.deepCopyOf(groupValue);
      }
    }

    @Override
    protected void setDocValuesSources(DocValues.Source source) {
      this.sortedSource = source.asSortedSource();
    }

    @Override
    protected DocValues.Source getDefaultSource(AtomicReaderContext readerContext) {
      return DocValues.getDefaultSortedSource(valueType, readerContext.reader().maxDoc());
    }
  }

}
