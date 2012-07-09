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
import org.apache.lucene.search.grouping.AbstractAllGroupsCollector;
import org.apache.lucene.util.SentinelIntSet;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.*;

/**
 * Implementation of {@link AbstractAllGroupsCollector} that groups documents based on
 * {@link DocValues} fields.
 *
 * @lucene.experimental
 */
public abstract class DVAllGroupsCollector<GROUP_VALUE_TYPE> extends AbstractAllGroupsCollector<GROUP_VALUE_TYPE> {

  private static final int DEFAULT_INITIAL_SIZE = 128;

  /**
   * Expert: Constructs a {@link DVAllGroupsCollector}.
   * Selects and constructs the most optimal all groups collector implementation for grouping by {@link DocValues}.
   * 
   *
   * @param groupField  The field to group by
   * @param type The {@link Type} which is used to select a concrete implementation.
   * @param diskResident Whether the values to group by should be disk resident
   * @param initialSize The initial allocation size of the
   *                    internal int set and group list
   *                    which should roughly match the total
   *                    number of expected unique groups. Be aware that the
   *                    heap usage is 4 bytes * initialSize. Not all concrete implementions use this!
   * @return the most optimal all groups collector implementation for grouping by {@link DocValues}
   */
  @SuppressWarnings("unchecked")
  public static <T> DVAllGroupsCollector<T> create(String groupField, DocValues.Type type, boolean diskResident, int initialSize) {
    switch (type) {
      case VAR_INTS:
      case FIXED_INTS_8:
      case FIXED_INTS_16:
      case FIXED_INTS_32:
      case FIXED_INTS_64:
        // Type erasure b/c otherwise we have inconvertible types...
        return (DVAllGroupsCollector) new Lng(groupField, type, diskResident);
      case FLOAT_32:
      case FLOAT_64:
        // Type erasure b/c otherwise we have inconvertible types...
        return (DVAllGroupsCollector) new Dbl(groupField, type, diskResident);
      case BYTES_FIXED_STRAIGHT:
      case BYTES_FIXED_DEREF:
      case BYTES_VAR_STRAIGHT:
      case BYTES_VAR_DEREF:
        // Type erasure b/c otherwise we have inconvertible types...
        return (DVAllGroupsCollector) new BR(groupField, type, diskResident);
      case BYTES_VAR_SORTED:
      case BYTES_FIXED_SORTED:
        // Type erasure b/c otherwise we have inconvertible types...
        return (DVAllGroupsCollector) new SortedBR(groupField, type, diskResident, initialSize);
      default:
        throw new IllegalArgumentException(String.format(Locale.ROOT, "ValueType %s not supported", type));
    }
  }

  /**
   * Constructs a {@link DVAllGroupsCollector}.
   * Selects and constructs the most optimal all groups collector implementation for grouping by {@link DocValues}.
   * If implementations require an initial allocation size then this will be set to 128.
   *
   *
   * @param groupField  The field to group by
   * @param type The {@link Type} which is used to select a concrete implementation.
   * @param diskResident Wether the values to group by should be disk resident
   * @return the most optimal all groups collector implementation for grouping by {@link DocValues}
   */
  public static <T> DVAllGroupsCollector<T> create(String groupField, DocValues.Type type, boolean diskResident) {
    return create(groupField, type, diskResident, DEFAULT_INITIAL_SIZE);
  }

  final String groupField;
  final DocValues.Type valueType;
  final boolean diskResident;
  final Collection<GROUP_VALUE_TYPE> groups;

  DVAllGroupsCollector(String groupField, DocValues.Type valueType, boolean diskResident, Collection<GROUP_VALUE_TYPE> groups) {
    this.groupField = groupField;
    this.valueType = valueType;
    this.diskResident = diskResident;
    this.groups = groups;
  }

  @Override
  public void setNextReader(AtomicReaderContext readerContext) throws IOException {
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

  static class Lng extends DVAllGroupsCollector<Long> {

    private DocValues.Source source;

    Lng(String groupField, DocValues.Type valueType, boolean diskResident) {
      super(groupField, valueType, diskResident, new TreeSet<Long>());
    }

    public void collect(int doc) throws IOException {
      long value = source.getInt(doc);
      if (!groups.contains(value)) {
        groups.add(value);
      }
    }

    public Collection<Long> getGroups() {
      return groups;
    }

    protected void setDocValuesSources(DocValues.Source source, AtomicReaderContext readerContext) {
      this.source = source;
    }

  }

  static class Dbl extends DVAllGroupsCollector<Double> {

    private DocValues.Source source;

    Dbl(String groupField, DocValues.Type valueType, boolean diskResident) {
      super(groupField, valueType, diskResident, new TreeSet<Double>());
    }

    public void collect(int doc) throws IOException {
      double value = source.getFloat(doc);
      if (!groups.contains(value)) {
        groups.add(value);
      }
    }

    public Collection<Double> getGroups() {
      return groups;
    }

    protected void setDocValuesSources(DocValues.Source source, AtomicReaderContext readerContext) {
      this.source = source;
    }

  }

  static class BR extends DVAllGroupsCollector<BytesRef> {

    private final BytesRef spare = new BytesRef();

    private DocValues.Source source;

    BR(String groupField, DocValues.Type valueType, boolean diskResident) {
      super(groupField, valueType, diskResident, new TreeSet<BytesRef>());
    }

    public void collect(int doc) throws IOException {
      BytesRef value = source.getBytes(doc, spare);
      if (!groups.contains(value)) {
        groups.add(BytesRef.deepCopyOf(value));
      }
    }

    public Collection<BytesRef> getGroups() {
      return groups;
    }

    protected void setDocValuesSources(DocValues.Source source, AtomicReaderContext readerContext) {
      this.source = source;
    }

  }

  static class SortedBR extends DVAllGroupsCollector<BytesRef> {

    private final SentinelIntSet ordSet;
    private final BytesRef spare = new BytesRef();

    private DocValues.SortedSource source;

    SortedBR(String groupField, DocValues.Type valueType, boolean diskResident, int initialSize) {
      super(groupField, valueType, diskResident, new ArrayList<BytesRef>(initialSize));
      ordSet = new SentinelIntSet(initialSize, -1);
    }

    public void collect(int doc) throws IOException {
      int ord = source.ord(doc);
      if (!ordSet.exists(ord)) {
        ordSet.put(ord);
        BytesRef value = source.getBytes(doc, new BytesRef());
        groups.add(value);
      }
    }

    public Collection<BytesRef> getGroups() {
      return groups;
    }

    protected void setDocValuesSources(DocValues.Source source, AtomicReaderContext readerContext) {
      this.source = source.asSortedSource();

      ordSet.clear();
      for (BytesRef countedGroup : groups) {
        int ord = this.source.getOrdByValue(countedGroup, spare);
        if (ord >= 0) {
          ordSet.put(ord);
        }
      }
    }

    @Override
    protected DocValues.Source getDefaultSource(AtomicReaderContext readerContext) {
      return DocValues.getDefaultSortedSource(valueType, readerContext.reader().maxDoc());
    }

  }

}
