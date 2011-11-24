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
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.AbstractFirstPassGroupingCollector;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * IDV based Implementations of {@link AbstractFirstPassGroupingCollector}.
 *
 * @lucene.experimental 
 */
public abstract class DVFirstPassGroupingCollector<GROUP_VALUE_TYPE> extends AbstractFirstPassGroupingCollector<GROUP_VALUE_TYPE> {

  final String groupField;
  final boolean diskResident;
  final ValueType valueType;

  public static DVFirstPassGroupingCollector create(Sort groupSort, int topNGroups, String groupField, ValueType type, boolean diskResident) throws IOException {
    switch (type) {
      case VAR_INTS:
      case FIXED_INTS_8:
      case FIXED_INTS_16:
      case FIXED_INTS_32:
      case FIXED_INTS_64:
        return new Lng(groupSort, topNGroups, groupField, diskResident, type);
      case FLOAT_32:
      case FLOAT_64:
        return new Dbl(groupSort, topNGroups, groupField, diskResident, type);
      case BYTES_FIXED_STRAIGHT:
      case BYTES_FIXED_DEREF:
      case BYTES_VAR_STRAIGHT:
      case BYTES_VAR_DEREF:
        return new BR(groupSort, topNGroups, groupField, diskResident, type);
      case BYTES_VAR_SORTED:
      case BYTES_FIXED_SORTED:
        return new SortedBR(groupSort, topNGroups, groupField, diskResident, type);
      default:
        throw new IllegalArgumentException(String.format("ValueType %s not supported", type));
    }
  }

  DVFirstPassGroupingCollector(Sort groupSort, int topNGroups, String groupField, boolean diskResident, ValueType valueType) throws IOException {
    super(groupSort, topNGroups);
    this.groupField = groupField;
    this.diskResident = diskResident;
    this.valueType = valueType;
  }

  @Override
  public void setNextReader(IndexReader.AtomicReaderContext readerContext) throws IOException {
    super.setNextReader(readerContext);

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

  static class Lng extends DVFirstPassGroupingCollector<Long> {

    private IndexDocValues.Source source;

    Lng(Sort groupSort, int topNGroups, String groupField, boolean diskResident, ValueType type) throws IOException {
      super(groupSort, topNGroups, groupField, diskResident, type);
    }

    protected Long getDocGroupValue(int doc) {
      return source.getInt(doc);
    }

    protected Long copyDocGroupValue(Long groupValue, Long reuse) {
      return groupValue;
    }

    protected void setDocValuesSources(IndexDocValues.Source source) {
      this.source = source;
    }
  }

  static class Dbl extends DVFirstPassGroupingCollector<Double> {

    private IndexDocValues.Source source;

    Dbl(Sort groupSort, int topNGroups, String groupField, boolean diskResident, ValueType type) throws IOException {
      super(groupSort, topNGroups, groupField, diskResident, type);
    }

    protected Double getDocGroupValue(int doc) {
      return source.getFloat(doc);
    }

    protected Double copyDocGroupValue(Double groupValue, Double reuse) {
      return groupValue;
    }

    protected void setDocValuesSources(IndexDocValues.Source source) {
      this.source = source;
    }
  }

  static class BR extends DVFirstPassGroupingCollector<BytesRef> {

    private IndexDocValues.Source source;
    private final BytesRef spare = new BytesRef();

    BR(Sort groupSort, int topNGroups, String groupField, boolean diskResident, ValueType type) throws IOException {
      super(groupSort, topNGroups, groupField, diskResident, type);
    }

    protected BytesRef getDocGroupValue(int doc) {
      return source.getBytes(doc, spare);
    }

    protected BytesRef copyDocGroupValue(BytesRef groupValue, BytesRef reuse) {
      if (reuse != null) {
        reuse.copy(groupValue);
        return reuse;
      } else {
        return new BytesRef(groupValue);
      }
    }

    @Override
    protected void setDocValuesSources(IndexDocValues.Source source) {
      this.source = source;
    }
  }

  static class SortedBR extends DVFirstPassGroupingCollector<BytesRef> {

    private IndexDocValues.SortedSource sortedSource;
    private final BytesRef spare = new BytesRef();

    SortedBR(Sort groupSort, int topNGroups, String groupField, boolean diskResident, ValueType type) throws IOException {
      super(groupSort, topNGroups, groupField, diskResident, type);
    }

    @Override
    protected BytesRef getDocGroupValue(int doc) {
      return sortedSource.getBytes(doc, spare);
    }

    @Override
    protected BytesRef copyDocGroupValue(BytesRef groupValue, BytesRef reuse) {
      if (reuse != null) {
        reuse.copy(groupValue);
        return reuse;
      } else {
        return new BytesRef(groupValue);
      }
    }

    @Override
    protected void setDocValuesSources(IndexDocValues.Source source) {
      this.sortedSource = source.asSortedSource();
    }

    @Override
    protected IndexDocValues.Source getDefaultSource(IndexReader.AtomicReaderContext readerContext) {
      return IndexDocValues.getDefaultSortedSource(valueType, readerContext.reader.maxDoc());
    }
  }

}
