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
package org.apache.lucene.index;


import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.document.ReferenceDocValuesField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.search.DocIdSetIterator.all;

/** <p>Buffers up pending int[] of in-segment docids for each doc, then sorts, packs and flushes when
 * segment flushes, using the same encoding as SortedNumericDocValues.  There are two flavors,
 * selected by the {@link org.apache.lucene.document.Field} attribute {@code reftype}, either {@code docid} or {@code
 * knn-graph}.  The {@code knn-graph} flavor models a connected, undirected, acyclic graph. Each
 * new document refers to previous documents, and links are made symmetric by this writer. So
 * the simple star graph 1-2-3 would be encoded as:</p>

 * <pre>
 *   1: [ 2 ]
 *   2: [ 1, 3 ]
 *   3: [ 2 ]
 * </pre>
 *
 * <p>The {@docid} flavor does not enforce symmetry, and allows forward references to documents that
 * have not yet been inserted. It can be said to model graphs more generally, allowing for directed,
 * disconnected, and cyclic graphs. In both cases, references to nonexistent or deleted documents
 * are purged when flushing and/or merging.</p>
 *
 * nocommit this is public unlike other DocValuesWriters so we can access it from DocValuesConsumer
 *
 * @lucene.internal
 */

public class ReferenceDocValuesWriter extends SNDVWriterBase {
  private final Counter iwBytesUsed;
  private final FieldInfo fieldInfo;
  private final boolean isGraph;
  private SortedMap<Integer, IntArray> allValues;
  private IntArray currentValues;
  private DocsWithFieldSet docsWithField = new DocsWithFieldSet();
  private int currentDocId;

  public ReferenceDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    // TODO: add some overhead for currentValues map initial size
    allValues = new TreeMap<>();
    currentDocId = -1;
    String refType = fieldInfo.getAttribute(ReferenceDocValuesField.REFTYPE_ATTR);
    isGraph = "knn-graph".equals(refType);
  }

  @Override
  public void addValue(int docID, long value) {
    if (value < 0 || value == docID) {
      // nocommit: where is the right place to enforce that value <= maxDoc? When merging value > docID occurs
      throw new IllegalArgumentException("ReferenceDocValues.addValue " + value + " is not a valid docID");
    }
    int ivalue = (int) value;
    IntArray referent = allValues.get(ivalue);
    long used;
    if (referent == null) {
      assert allValues.isEmpty() : "ReferenceDocValues reference to document not in graph: " + value;
      // We must special-case the first entry. Because we require forward-only iteration (why?) we need
      // to bootstrap the nonexistent root node
      referent = new IntArray(docID);
      allValues.put(ivalue, referent);
      used = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 4 + RamUsageEstimator.sizeOf(referent.values);
      currentDocId = ivalue;
    } else {
      used = referent.add(docID);
    }
    if (docID != currentDocId) {
      // during merge this is not the case
      // assert docID > currentDocId;
      currentValues = new IntArray(ivalue);
      allValues.put(docID, currentValues);
      currentDocId = docID;
      // nocommit improve this estimate
      used += RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 4 + RamUsageEstimator.sizeOf(currentValues.values);
    } else {
      used += currentValues.add(ivalue);
    }
    iwBytesUsed.addAndGet(used);
  }

  boolean hasValue(int docID) {
    return allValues.containsKey(docID);
  }

  @Override
  public void finish(int maxDoc) {
  }

  @Override
  Sorter.DocComparator getDocComparator(int maxDoc, SortField sortField) {
    throw new IllegalArgumentException("It is forbidden to sort by a ReferenceDocValues field");
  }

  @Override
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer dvConsumer) throws IOException {
    PackedLongValues.Builder valuesBuilder = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT); // stream of all values
    PackedLongValues.Builder countsBuilder = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);; // count of values per doc
    for (Map.Entry<Integer, IntArray> e : allValues.entrySet()) {
      IntArray values = e.getValue();
      // record the values for this doc
      if (sortMap != null) {
        // If the index is sorted, remap the references
        for (int i = 0; i < values.size; i++) {
          values.values[i] = sortMap.oldToNew(values.values[i]);
        }
      }
      // Sort the values in ascending order as required by SortedNumericDocValues format
      values.sort();
      int lastValue = -1;
      long countBefore = valuesBuilder.size();
      for (int i = 0; i < values.size; i++) {
        int value = values.values[i];
        if (value != lastValue) {
          // eliminate duplicate values
          lastValue = value;
          if (isGraph || state.liveDocs == null || state.liveDocs.get(value)) {
            // drop references to deleted documents, unless this is a knn-graph, when we must retain
            // them in order to preserve the graph-connectivity
            valuesBuilder.add(value);
          }
        }
      }
      long numAdded = valuesBuilder.size() - countBefore;
      // record the number of values for this doc
      if (numAdded > 0) {
        // may be zero if deletions caused all references to be dropped
        docsWithField.add(e.getKey());
        countsBuilder.add(numAdded);
      }
    }

    final PackedLongValues values = valuesBuilder.build();
    final PackedLongValues valueCounts = countsBuilder.build();

    final long[][] sorted;
    if (sortMap != null) {
      sorted = SortedNumericDocValuesWriter.sortDocValues(state.segmentInfo.maxDoc(), sortMap,
          new SortedNumericDocValuesWriter.BufferedSortedNumericDocValues(values, valueCounts, docsWithField.iterator()));
    } else {
      sorted = null;
    }

    dvConsumer.addSortedNumericField(fieldInfo,
        new EmptyDocValuesProducer() {
          @Override
          public SortedNumericDocValues getSortedNumeric(FieldInfo fieldInfoIn) {
            if (fieldInfoIn != fieldInfo) {
              throw new IllegalArgumentException("wrong fieldInfo");
            }
            final SortedNumericDocValues buf =
              new SortedNumericDocValuesWriter.BufferedSortedNumericDocValues(values, valueCounts, docsWithField.iterator());
            if (sorted == null) {
              return buf;
            } else {
              return new SortingLeafReader.SortingSortedNumericDocValues(buf, sorted);
            }
          }
        });
    // Is this re-used??
    allValues.clear();
    docsWithField = null;
  }

  @Override
  DocIdSetIterator getDocIdSet() {
    return docsWithField.iterator();
  }

  public SortedNumericDocValues getIterableValues() {
    return new IterableValues();
  }

  public SortedNumericDocValues getBufferedValues() {
    return new RandomAccessValues();
  }

  private abstract class BufferedReferenceDocValues extends SortedNumericDocValues {
    int docID = -1;
    IntArray values;
    int valueUpTo;

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int docValueCount() {
      return values.size;
    }

    @Override
    public long nextValue() {
      return values.values[valueUpTo++];
    }

    @Override
    public long cost() {
      return docsWithField.cost();
    }
  }

  private class IterableValues extends BufferedReferenceDocValues {
    private final Iterator<Map.Entry<Integer, IntArray>> iterator = allValues.entrySet().iterator();

    @Override
    public int nextDoc() throws IOException {
      if (iterator.hasNext()) {
        Map.Entry<Integer, IntArray> entry = iterator.next();
        docID = entry.getKey();
        values = entry.getValue();
        // Sort the values in ascending order as required by SortedNumericDocValues format
        // Ideally we only create a single one of these iterators, but TODO: create a finish() method and do this
        // when we are done creating the merged writer
        values.sort();
        valueUpTo = 0;
      } else {
        docID = NO_MORE_DOCS;
        values = null;
      }
      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

  }

  private class RandomAccessValues extends BufferedReferenceDocValues {

    @Override
    public int nextDoc() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      values = allValues.get(target);
      if (values != null) {
        docID = target;
        valueUpTo = 0;
        return true;
      } else {
        docID = NO_MORE_DOCS;
        return false;
      }
    }

  }

  private static class IntArray {
    int size;
    int[] values = new int[8];

    IntArray(int first) {
      values[0] = first;
      size = 1;
    }

    /**
     * @param value the value to append to the array
     * @return bytes allocated if the array was resized, or zero
     */

    int add(int value) {
      int used;
      if (size == values.length) {
        values = ArrayUtil.grow(values, size + 1);
        values[size++] = value;
        used = (values.length - size) * 4;
      } else {
        used = 0;
      }
      values[size++] = value;
      return used;
    }

    void sort() {
      Arrays.sort(values, 0, size);
    }
  }

}

