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
import org.apache.lucene.search.grouping.AbstractDistinctValuesCollector;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SentinelIntSet;
import org.apache.lucene.index.DocValues.Type; // javadocs

import java.io.IOException;
import java.util.*;

/**
 * Docvalues implementation of {@link org.apache.lucene.search.grouping.AbstractDistinctValuesCollector}.
 *
 * @lucene.experimental
 */
public abstract class DVDistinctValuesCollector<GC extends AbstractDistinctValuesCollector.GroupCount<?>> extends AbstractDistinctValuesCollector<GC> {

  final String groupField;
  final String countField;
  final boolean diskResident;
  final Type valueType;

  DVDistinctValuesCollector(String groupField, String countField, boolean diskResident, Type valueType) {
    this.groupField = groupField;
    this.countField = countField;
    this.diskResident = diskResident;
    this.valueType = valueType;
  }

  /**
   * Constructs a docvalues based implementation of {@link org.apache.lucene.search.grouping.AbstractDistinctValuesCollector} based on the specified
   * type.
   *
   * @param groupField    The field to group by
   * @param countField    The field to count distinct values for
   * @param groups        The top N groups, collected during the first phase search
   * @param diskResident  Whether the values to group and count by should be disk resident
   * @param type          The {@link Type} which is used to select a concrete implementation
   * @return a docvalues based distinct count collector
   */
  @SuppressWarnings("unchecked")
  public static <T> DVDistinctValuesCollector<GroupCount<T>> create(String groupField, String countField, Collection<SearchGroup<T>> groups, boolean diskResident, Type type) {
    switch (type) {
      case VAR_INTS:
      case FIXED_INTS_8:
      case FIXED_INTS_16:
      case FIXED_INTS_32:
      case FIXED_INTS_64:
        // Type erasure b/c otherwise we have inconvertible types...
        return (DVDistinctValuesCollector) new NonSorted.Lng(groupField, countField, (Collection) groups, diskResident, type);
      case FLOAT_32:
      case FLOAT_64:
        // Type erasure b/c otherwise we have inconvertible types...
        return (DVDistinctValuesCollector) new NonSorted.Dbl(groupField, countField, (Collection) groups, diskResident, type);
      case BYTES_FIXED_STRAIGHT:
      case BYTES_FIXED_DEREF:
      case BYTES_VAR_STRAIGHT:
      case BYTES_VAR_DEREF:
        // Type erasure b/c otherwise we have inconvertible types...
        return (DVDistinctValuesCollector) new NonSorted.BR(groupField, countField, (Collection) groups, diskResident, type);
      case BYTES_VAR_SORTED:
      case BYTES_FIXED_SORTED:
        // Type erasure b/c otherwise we have inconvertible types...
        return (DVDistinctValuesCollector) new Sorted.BR(groupField, countField, (Collection) groups, diskResident, type);
      default:
        throw new IllegalArgumentException(String.format(Locale.ROOT, "ValueType %s not supported", type));
    }
  }


  static abstract class NonSorted<K> extends DVDistinctValuesCollector<NonSorted.GroupCount> {

    final Map<K, GroupCount> groupMap = new LinkedHashMap<K, GroupCount>();

    DocValues.Source groupFieldSource;
    DocValues.Source countFieldSource;

    NonSorted(String groupField, String countField, boolean diskResident, Type valueType) {
      super(groupField, countField, diskResident, valueType);
    }

    @Override
    public List<GroupCount> getGroups() {
      return new ArrayList<GroupCount>(groupMap.values());
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
      groupFieldSource = retrieveSource(groupField, context);
      countFieldSource = retrieveSource(countField, context);
    }

    private DocValues.Source retrieveSource(String fieldName, AtomicReaderContext context) throws IOException {
      DocValues groupFieldDv = context.reader().docValues(fieldName);
      if (groupFieldDv != null) {
        return diskResident ? groupFieldDv.getDirectSource() : groupFieldDv.getSource();
      } else {
        return DocValues.getDefaultSource(valueType);
      }
    }

    static class Dbl extends NonSorted<Double> {

      Dbl(String groupField, String countField, Collection<SearchGroup<Double>> groups, boolean diskResident, Type valueType) {
        super(groupField, countField, diskResident, valueType);
        for (SearchGroup<Double> group : groups) {
          groupMap.put(group.groupValue, new GroupCount(group.groupValue));
        }
      }

      @Override
      public void collect(int doc) throws IOException {
        GroupCount groupCount = groupMap.get(groupFieldSource.getFloat(doc));
        if (groupCount != null) {
          groupCount.uniqueValues.add(countFieldSource.getFloat(doc));
        }
      }

    }

    static class Lng extends NonSorted<Long> {

      Lng(String groupField, String countField, Collection<SearchGroup<Long>> groups, boolean diskResident, Type valueType) {
        super(groupField, countField, diskResident, valueType);
        for (SearchGroup<Long> group : groups) {
          groupMap.put(group.groupValue, new GroupCount(group.groupValue));
        }
      }

      @Override
      public void collect(int doc) throws IOException {
        GroupCount groupCount = groupMap.get(groupFieldSource.getInt(doc));
        if (groupCount != null) {
          groupCount.uniqueValues.add(countFieldSource.getInt(doc));
        }
      }

    }

    static class BR extends NonSorted<BytesRef> {

      private final BytesRef spare = new BytesRef();

      BR(String groupField, String countField, Collection<SearchGroup<BytesRef>> groups, boolean diskResident, Type valueType) {
        super(groupField, countField, diskResident, valueType);
        for (SearchGroup<BytesRef> group : groups) {
          groupMap.put(group.groupValue, new GroupCount(group.groupValue));
        }
      }

      @Override
      public void collect(int doc) throws IOException {
        GroupCount groupCount = groupMap.get(groupFieldSource.getBytes(doc, spare));
        if (groupCount != null) {
          BytesRef countValue = countFieldSource.getBytes(doc, spare);
          if (!groupCount.uniqueValues.contains(countValue)) {
            groupCount.uniqueValues.add(BytesRef.deepCopyOf(countValue));
          }
        }
      }

    }

    static class GroupCount extends AbstractDistinctValuesCollector.GroupCount<Comparable<?>> {

      GroupCount(Comparable<?> groupValue) {
        super(groupValue);
      }

    }

  }


  static abstract class Sorted extends DVDistinctValuesCollector<Sorted.GroupCount> {

    final SentinelIntSet ordSet;
    final GroupCount groupCounts[];
    final List<GroupCount> groups = new ArrayList<GroupCount>();

    DocValues.SortedSource groupFieldSource;
    DocValues.SortedSource countFieldSource;

    Sorted(String groupField, String countField, int groupSize, boolean diskResident, Type valueType) {
      super(groupField, countField, diskResident, valueType);
      ordSet = new SentinelIntSet(groupSize, -1);
      groupCounts = new GroupCount[ordSet.keys.length];
    }

    @Override
    public List<GroupCount> getGroups() {
      return groups;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
      groupFieldSource = retrieveSortedSource(groupField, context);
      countFieldSource = retrieveSortedSource(countField, context);
      ordSet.clear();
    }

    private DocValues.SortedSource retrieveSortedSource(String field, AtomicReaderContext context) throws IOException {
      DocValues countFieldDv = context.reader().docValues(field);
      if (countFieldDv != null) {
        return diskResident ? countFieldDv.getDirectSource().asSortedSource() : countFieldDv.getSource().asSortedSource();
      } else {
        return DocValues.getDefaultSortedSource(valueType, context.reader().maxDoc());
      }
    }

    static class BR extends Sorted {

      final BytesRef spare = new BytesRef();

      BR(String groupField, String countField, Collection<SearchGroup<BytesRef>> searchGroups, boolean diskResident, Type valueType) {
        super(groupField, countField, searchGroups.size(), diskResident, valueType);
        for (SearchGroup<BytesRef> group : searchGroups) {
          this.groups.add(new GroupCount(group.groupValue));
        }
      }

      @Override
      public void collect(int doc) throws IOException {
        int slot = ordSet.find(groupFieldSource.ord(doc));
        if (slot < 0) {
          return;
        }

        GroupCount gc = groupCounts[slot];
        int countOrd = countFieldSource.ord(doc);
        if (doesNotContainsOrd(countOrd, gc.ords)) {
          gc.uniqueValues.add(countFieldSource.getByOrd(countOrd, new BytesRef()));
          gc.ords = Arrays.copyOf(gc.ords, gc.ords.length + 1);
          gc.ords[gc.ords.length - 1] = countOrd;
          if (gc.ords.length > 1) {
            Arrays.sort(gc.ords);
          }
        }
      }

      private boolean doesNotContainsOrd(int ord, int[] ords) {
        if (ords.length == 0) {
          return true;
        } else if (ords.length == 1) {
          return ord != ords[0];
        }
        return Arrays.binarySearch(ords, ord) < 0;
      }

      @Override
      public void setNextReader(AtomicReaderContext context) throws IOException {
        super.setNextReader(context);
        for (GroupCount group : groups) {
          int groupOrd = groupFieldSource.getOrdByValue((BytesRef) group.groupValue, spare);
          if (groupOrd < 0) {
            continue;
          }

          groupCounts[ordSet.put(groupOrd)] = group;
          group.ords = new int[group.uniqueValues.size()];
          Arrays.fill(group.ords, -1);
          int i = 0;
          for (Comparable<?> value : group.uniqueValues) {
            int countOrd = countFieldSource.getOrdByValue((BytesRef) value, spare);
            if (countOrd >= 0) {
              group.ords[i++] = countOrd;
            }
          }
        }
      }
    }

    static class GroupCount extends AbstractDistinctValuesCollector.GroupCount<Comparable<?>> {

      int[] ords;

      GroupCount(Comparable<?> groupValue) {
        super(groupValue);
      }

    }

  }

}
