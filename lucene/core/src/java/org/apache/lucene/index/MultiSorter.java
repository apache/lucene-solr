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
import java.util.List;

import org.apache.lucene.index.MergeState.DocMap;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

final class MultiSorter {
  
  /** Does a merge sort of the leaves of the incoming reader, returning {@link DocMap} to map each leaf's
   *  documents into the merged segment.  The documents for each incoming leaf reader must already be sorted by the same sort!
   *  Returns null if the merge sort is not needed (segments are already in index sort order).
   **/
  static MergeState.DocMap[] sort(Sort sort, List<CodecReader> readers) throws IOException {

    // TODO: optimize if only 1 reader is incoming, though that's a rare case

    SortField fields[] = sort.getSort();
    final IndexSorter.ComparableProvider[][] comparables = new IndexSorter.ComparableProvider[fields.length][];
    final int[] reverseMuls = new int[fields.length];
    for(int i=0;i<fields.length;i++) {
      IndexSorter sorter = fields[i].getIndexSorter();
      if (sorter == null) {
        throw new IllegalArgumentException("Cannot use sort field " + fields[i] + " for index sorting");
      }
      comparables[i] = sorter.getComparableProviders(readers);
      reverseMuls[i] = fields[i].getReverse() ? -1 : 1;
    }
    int leafCount = readers.size();

    PriorityQueue<LeafAndDocID> queue = new PriorityQueue<LeafAndDocID>(leafCount) {
        @Override
        public boolean lessThan(LeafAndDocID a, LeafAndDocID b) {
          for(int i=0;i<comparables.length;i++) {
            int cmp = Long.compare(a.valuesAsComparableLongs[i], b.valuesAsComparableLongs[i]);
            if (cmp != 0) {
              return reverseMuls[i] * cmp < 0;
            }
          }

          // tie-break by docID natural order:
          if (a.readerIndex != b.readerIndex) {
            return a.readerIndex < b.readerIndex;
          } else {
            return a.docID < b.docID;
          }
        }
    };

    PackedLongValues.Builder[] builders = new PackedLongValues.Builder[leafCount];

    for(int i=0;i<leafCount;i++) {
      CodecReader reader = readers.get(i);
      LeafAndDocID leaf = new LeafAndDocID(i, reader.getLiveDocs(), reader.maxDoc(), comparables.length);
      for(int j=0;j<comparables.length;j++) {
        leaf.valuesAsComparableLongs[j] = comparables[j][i].getAsComparableLong(leaf.docID);
      }
      queue.add(leaf);
      builders[i] = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
    }

    // merge sort:
    int mappedDocID = 0;
    int lastReaderIndex = 0;
    boolean isSorted = true;
    while (queue.size() != 0) {
      LeafAndDocID top = queue.top();
      if (lastReaderIndex > top.readerIndex) {
        // merge sort is needed
        isSorted = false;
      }
      lastReaderIndex = top.readerIndex;
      builders[top.readerIndex].add(mappedDocID);
      if (top.liveDocs == null || top.liveDocs.get(top.docID)) {
        mappedDocID++;
      }
      top.docID++;
      if (top.docID < top.maxDoc) {
        for(int j=0;j<comparables.length;j++) {
          top.valuesAsComparableLongs[j] = comparables[j][top.readerIndex].getAsComparableLong(top.docID);
        }
        queue.updateTop();
      } else {
        queue.pop();
      }
    }
    if (isSorted) {
      return null;
    }

    MergeState.DocMap[] docMaps = new MergeState.DocMap[leafCount];
    for(int i=0;i<leafCount;i++) {
      final PackedLongValues remapped = builders[i].build();
      final Bits liveDocs = readers.get(i).getLiveDocs();
      docMaps[i] = new MergeState.DocMap() {
          @Override
          public int get(int docID) {
            if (liveDocs == null || liveDocs.get(docID)) {
              return (int) remapped.get(docID);
            } else {
              return -1;
            }
          }
        };
    }

    return docMaps;
  }

  private static class LeafAndDocID {
    final int readerIndex;
    final Bits liveDocs;
    final int maxDoc;
    final long[] valuesAsComparableLongs;
    int docID;

    public LeafAndDocID(int readerIndex, Bits liveDocs, int maxDoc, int numComparables) {
      this.readerIndex = readerIndex;
      this.liveDocs = liveDocs;
      this.maxDoc = maxDoc;
      this.valuesAsComparableLongs = new long[numComparables];
    }
  }
}
