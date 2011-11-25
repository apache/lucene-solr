package org.apache.lucene.index.values;

/**
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.MergeState.IndexReaderAndLiveDocs;
import org.apache.lucene.index.values.IndexDocValues.SortedSource;
import org.apache.lucene.index.values.IndexDocValues.Source;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.packed.PackedInts;

/**
 * @lucene.internal
 */
final class SortedBytesMergeUtils {

  private SortedBytesMergeUtils() {
    // no instance
  }

  static MergeContext init(ValueType type, IndexDocValues[] docValues,
      Comparator<BytesRef> comp, MergeState mergeState) {
    int size = -1;
    if (type == ValueType.BYTES_FIXED_SORTED) {
      for (IndexDocValues indexDocValues : docValues) {
        if (indexDocValues != null) {
          size = indexDocValues.getValueSize();
          break;
        }
      }
      assert size >= 0;
    }
    return new MergeContext(comp, mergeState, size, type);
  }

  public static final class MergeContext {
    private final Comparator<BytesRef> comp;
    private final BytesRef missingValue = new BytesRef();
    final int sizePerValues; // -1 if var length
    final ValueType type;
    final int[] docToEntry;
    long[] offsets; // if non-null #mergeRecords collects byte offsets here

    public MergeContext(Comparator<BytesRef> comp, MergeState mergeState,
        int size, ValueType type) {
      assert type == ValueType.BYTES_FIXED_SORTED || type == ValueType.BYTES_VAR_SORTED;
      this.comp = comp;
      this.sizePerValues = size;
      this.type = type;
      if (size > 0) {
        missingValue.grow(size);
        missingValue.length = size;
      }
      docToEntry = new int[mergeState.mergedDocCount];
    }
  }

  static List<SortedSourceSlice> buildSlices(MergeState mergeState,
      IndexDocValues[] docValues, MergeContext ctx) throws IOException {
    final List<SortedSourceSlice> slices = new ArrayList<SortedSourceSlice>();
    for (int i = 0; i < docValues.length; i++) {
      final SortedSourceSlice nextSlice;
      final Source directSource;
      if (docValues[i] != null
          && (directSource = docValues[i].getDirectSource()) != null) {
        final SortedSourceSlice slice = new SortedSourceSlice(i, directSource
            .asSortedSource(), mergeState, ctx.docToEntry);
        nextSlice = slice;
      } else {
        nextSlice = new SortedSourceSlice(i, new MissingValueSource(ctx),
            mergeState, ctx.docToEntry);
      }
      createOrdMapping(mergeState, nextSlice);
      slices.add(nextSlice);
    }
    return Collections.unmodifiableList(slices);
  }

  /*
   * In order to merge we need to map the ords used in each segment to the new
   * global ords in the new segment. Additionally we need to drop values that
   * are not referenced anymore due to deleted documents. This method walks all
   * live documents and fetches their current ordinal. We store this ordinal per
   * slice and (SortedSourceSlice#ordMapping) and remember the doc to ord
   * mapping in docIDToRelativeOrd. After the merge SortedSourceSlice#ordMapping
   * contains the new global ordinals for the relative index.
   */
  private static void createOrdMapping(MergeState mergeState,
      SortedSourceSlice currentSlice) {
    final int readerIdx = currentSlice.readerIdx;
    final int[] currentDocMap = mergeState.docMaps[readerIdx];
    final int docBase = currentSlice.docToOrdStart;
    assert docBase == mergeState.docBase[readerIdx];
    if (currentDocMap != null) { // we have deletes
      for (int i = 0; i < currentDocMap.length; i++) {
        final int doc = currentDocMap[i];
        if (doc != -1) { // not deleted
          final int ord = currentSlice.source.ord(i); // collect ords strictly
                                                      // increasing
          currentSlice.docIDToRelativeOrd[docBase + doc] = ord;
          // use ord + 1 to identify unreferenced values (ie. == 0)
          currentSlice.ordMapping[ord] = ord + 1;
        }
      }
    } else { // no deletes
      final IndexReaderAndLiveDocs indexReaderAndLiveDocs = mergeState.readers
          .get(readerIdx);
      final int numDocs = indexReaderAndLiveDocs.reader.numDocs();
      assert indexReaderAndLiveDocs.liveDocs == null;
      assert currentSlice.docToOrdEnd - currentSlice.docToOrdStart == numDocs;
      for (int doc = 0; doc < numDocs; doc++) {
        final int ord = currentSlice.source.ord(doc);
        currentSlice.docIDToRelativeOrd[docBase + doc] = ord;
        // use ord + 1 to identify unreferenced values (ie. == 0)
        currentSlice.ordMapping[ord] = ord + 1;
      }
    }
  }

  static int mergeRecords(MergeContext ctx, IndexOutput datOut,
      List<SortedSourceSlice> slices) throws IOException {
    final RecordMerger merger = new RecordMerger(new MergeQueue(slices.size(),
        ctx.comp), slices.toArray(new SortedSourceSlice[0]));
    long[] offsets = ctx.offsets;
    final boolean recordOffsets = offsets != null;
    long offset = 0;
    BytesRef currentMergedBytes;
    merger.pushTop();
    while (merger.queue.size() > 0) {
      merger.pullTop();
      currentMergedBytes = merger.current;
      assert ctx.sizePerValues == -1 || ctx.sizePerValues == currentMergedBytes.length : "size: "
          + ctx.sizePerValues + " spare: " + currentMergedBytes.length;

      if (recordOffsets) {
        offset += currentMergedBytes.length;
        if (merger.currentOrd >= offsets.length) {
          offsets = ArrayUtil.grow(offsets, merger.currentOrd + 1);
        }
        offsets[merger.currentOrd] = offset;
      }
      datOut.writeBytes(currentMergedBytes.bytes, currentMergedBytes.offset,
          currentMergedBytes.length);
      merger.pushTop();
    }
    ctx.offsets = offsets;
    assert offsets == null || offsets[merger.currentOrd - 1] == offset;
    return merger.currentOrd;
  }

  private static final class RecordMerger {
    private final MergeQueue queue;
    private final SortedSourceSlice[] top;
    private int numTop;
    BytesRef current;
    int currentOrd = -1;

    RecordMerger(MergeQueue queue, SortedSourceSlice[] top) {
      super();
      this.queue = queue;
      this.top = top;
      this.numTop = top.length;
    }

    private void pullTop() {
      // extract all subs from the queue that have the same
      // top record
      assert numTop == 0;
      assert currentOrd >= 0;
      while (true) {
        final SortedSourceSlice popped = top[numTop++] = queue.pop();
        // use ord + 1 to identify unreferenced values (ie. == 0)
        popped.ordMapping[popped.relativeOrd] = currentOrd + 1;
        if (queue.size() == 0
            || !(queue.top()).current.bytesEquals(top[0].current)) {
          break;
        }
      }
      current = top[0].current;
    }

    private void pushTop() throws IOException {
      // call next() on each top, and put back into queue
      for (int i = 0; i < numTop; i++) {
        top[i].current = top[i].next();
        if (top[i].current != null) {
          queue.add(top[i]);
        }
      }
      currentOrd++;
      numTop = 0;
    }
  }

  static class SortedSourceSlice {
    final SortedSource source;
    final int readerIdx;
    /* global array indexed by docID containg the relative ord for the doc */
    final int[] docIDToRelativeOrd;
    /*
     * maps relative ords to merged global ords - index is relative ord value
     * new global ord this map gets updates as we merge ords. later we use the
     * docIDtoRelativeOrd to get the previous relative ord to get the new ord
     * from the relative ord map.
     */
    final int[] ordMapping;

    /* start index into docIDToRelativeOrd */
    final int docToOrdStart;
    /* end index into docIDToRelativeOrd */
    final int docToOrdEnd;
    BytesRef current = new BytesRef();
    /* the currently merged relative ordinal */
    int relativeOrd = -1;

    SortedSourceSlice(int readerIdx, SortedSource source, MergeState state,
        int[] docToOrd) {
      super();
      this.readerIdx = readerIdx;
      this.source = source;
      this.docIDToRelativeOrd = docToOrd;
      this.ordMapping = new int[source.getValueCount()];
      this.docToOrdStart = state.docBase[readerIdx];
      this.docToOrdEnd = this.docToOrdStart + numDocs(state, readerIdx);
    }

    private static int numDocs(MergeState state, int readerIndex) {
      if (readerIndex == state.docBase.length - 1) {
        return state.mergedDocCount - state.docBase[readerIndex];
      }
      return state.docBase[readerIndex + 1] - state.docBase[readerIndex];
    }

    BytesRef next() {
      for (int i = relativeOrd + 1; i < ordMapping.length; i++) {
        if (ordMapping[i] != 0) { // skip ords that are not referenced anymore
          source.getByOrd(i, current);
          relativeOrd = i;
          return current;
        }
      }
      return null;
    }

    void writeOrds(PackedInts.Writer writer) throws IOException {
      for (int i = docToOrdStart; i < docToOrdEnd; i++) {
        final int mappedOrd = docIDToRelativeOrd[i];
        assert mappedOrd < ordMapping.length;
        assert ordMapping[mappedOrd] > 0 : "illegal mapping ord maps to an unreferenced value";
        writer.add(ordMapping[mappedOrd] - 1);
      }
    }
  }

  /*
   * if a segment has no values at all we use this source to fill in the missing
   * value in the right place (depending on the comparator used)
   */
  private static final class MissingValueSource extends SortedSource {

    private BytesRef missingValue;

    public MissingValueSource(MergeContext ctx) {
      super(ctx.type, ctx.comp);
      this.missingValue = ctx.missingValue;
    }

    @Override
    public int ord(int docID) {
      return 0;
    }

    @Override
    public BytesRef getByOrd(int ord, BytesRef bytesRef) {
      bytesRef.copyBytes(missingValue);
      return bytesRef;
    }

    @Override
    public PackedInts.Reader getDocToOrd() {
      return null;
    }

    @Override
    public int getValueCount() {
      return 1;
    }

  }

  /*
   * merge queue
   */
  private static final class MergeQueue extends
      PriorityQueue<SortedSourceSlice> {
    final Comparator<BytesRef> comp;

    public MergeQueue(int maxSize, Comparator<BytesRef> comp) {
      super(maxSize);
      this.comp = comp;
    }

    @Override
    protected boolean lessThan(SortedSourceSlice a, SortedSourceSlice b) {
      int cmp = comp.compare(a.current, b.current);
      if (cmp != 0) {
        return cmp < 0;
      } else { // just a tie-breaker
        return a.docToOrdStart < b.docToOrdStart;
      }
    }

  }
}
