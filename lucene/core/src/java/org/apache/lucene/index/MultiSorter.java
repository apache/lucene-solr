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
import java.util.ArrayList;
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
    final CrossReaderComparator[] comparators = new CrossReaderComparator[fields.length];
    for(int i=0;i<fields.length;i++) {
      comparators[i] = getComparator(readers, fields[i]);
    }

    int leafCount = readers.size();

    PriorityQueue<LeafAndDocID> queue = new PriorityQueue<LeafAndDocID>(leafCount) {
        @Override
        public boolean lessThan(LeafAndDocID a, LeafAndDocID b) {
          for(int i=0;i<comparators.length;i++) {
            int cmp = comparators[i].compare(a.readerIndex, a.docID, b.readerIndex, b.docID);
            if (cmp != 0) {
              return cmp < 0;
            }
          }

          // tie-break by docID natural order:
          if (a.readerIndex != b.readerIndex) {
            return a.readerIndex < b.readerIndex;
          }
          return a.docID < b.docID;
        }
    };

    PackedLongValues.Builder[] builders = new PackedLongValues.Builder[leafCount];

    for(int i=0;i<leafCount;i++) {
      CodecReader reader = readers.get(i);
      queue.add(new LeafAndDocID(i, reader.getLiveDocs(), reader.maxDoc()));
      builders[i] = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
    }

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
    int docID;

    public LeafAndDocID(int readerIndex, Bits liveDocs, int maxDoc) {
      this.readerIndex = readerIndex;
      this.liveDocs = liveDocs;
      this.maxDoc = maxDoc;
    }
  }

  private interface CrossReaderComparator {
    public int compare(int readerIndexA, int docIDA, int readerIndexB, int docIDB);
  }

  /** Returns {@code CrossReaderComparator} for the provided readers to represent the requested {@link SortField} sort order. */
  private static CrossReaderComparator getComparator(List<CodecReader> readers, SortField sortField) throws IOException {
    final int reverseMul = sortField.getReverse() ? -1 : 1;
    final SortField.Type sortType = Sorter.getSortFieldType(sortField);

    switch(sortType) {

    case STRING:
      {
        // this uses the efficient segment-local ordinal map:
        MultiReader multiReader = new MultiReader(readers.toArray(new LeafReader[readers.size()]));
        final int[] docStarts = new int[readers.size()+1];
        List<LeafReaderContext> leaves = multiReader.leaves();
        final SortedDocValues[] leafValues = new SortedDocValues[readers.size()];
        for(int i=0;i<readers.size();i++) {
          leafValues[i] = Sorter.getOrWrapSorted(readers.get(i), sortField);
          docStarts[i] = leaves.get(i).docBase;
        }
        docStarts[readers.size()] = multiReader.maxDoc();
        final SortedDocValues sorted = MultiDocValues.getSortedValues(multiReader, leafValues, docStarts);
        final int missingOrd;
        // no need to take sortField.getReverse() into account here since we apply reverseMul on top of the ordinal comparison later on
        if (sortField.getMissingValue() == SortField.STRING_LAST) {
          missingOrd = Integer.MAX_VALUE;
        } else {
          missingOrd = Integer.MIN_VALUE;
        }

        return new CrossReaderComparator() {
          @Override
          public int compare(int readerIndexA, int docIDA, int readerIndexB, int docIDB) {
            int ordA = sorted.getOrd(docStarts[readerIndexA] + docIDA);
            if (ordA == -1) {
              ordA = missingOrd;
            }
            int ordB = sorted.getOrd(docStarts[readerIndexB] + docIDB);
            if (ordB == -1) {
              ordB = missingOrd;
            }
            return reverseMul * Integer.compare(ordA, ordB);
          }
        };
      }

    case LONG:
      {
        List<NumericDocValues> values = new ArrayList<>();
        List<Bits> docsWithFields = new ArrayList<>();
        for(CodecReader reader : readers) {
          values.add(Sorter.getOrWrapNumeric(reader, sortField));
          docsWithFields.add(DocValues.getDocsWithField(reader, sortField.getField()));
        }

        final Long missingValue;
        if (sortField.getMissingValue() != null) {
          missingValue = (Long) sortField.getMissingValue();
        } else {
          missingValue = 0l;
        }

        return new CrossReaderComparator() {
          @Override
          public int compare(int readerIndexA, int docIDA, int readerIndexB, int docIDB) {
            long valueA;
            if (docsWithFields.get(readerIndexA).get(docIDA)) {
              valueA = values.get(readerIndexA).get(docIDA);
            } else {
              valueA = missingValue;
            }

            long valueB;
            if (docsWithFields.get(readerIndexB).get(docIDB)) {
              valueB = values.get(readerIndexB).get(docIDB);
            } else {
              valueB = missingValue;
            }
            return reverseMul * Long.compare(valueA, valueB);
          }
        };
      }

    case INT:
      {
        List<NumericDocValues> values = new ArrayList<>();
        List<Bits> docsWithFields = new ArrayList<>();
        for(CodecReader reader : readers) {
          values.add(Sorter.getOrWrapNumeric(reader, sortField));
          docsWithFields.add(DocValues.getDocsWithField(reader, sortField.getField()));
        }

        final Integer missingValue;
        if (sortField.getMissingValue() != null) {
          missingValue = (Integer) sortField.getMissingValue();
        } else {
          missingValue = 0;
        }

        return new CrossReaderComparator() {
          @Override
          public int compare(int readerIndexA, int docIDA, int readerIndexB, int docIDB) {
            int valueA;
            if (docsWithFields.get(readerIndexA).get(docIDA)) {
              valueA = (int) values.get(readerIndexA).get(docIDA);
            } else {
              valueA = missingValue;
            }

            int valueB;
            if (docsWithFields.get(readerIndexB).get(docIDB)) {
              valueB = (int) values.get(readerIndexB).get(docIDB);
            } else {
              valueB = missingValue;
            }
            return reverseMul * Integer.compare(valueA, valueB);
          }
        };
      }

    case DOUBLE:
      {
        List<NumericDocValues> values = new ArrayList<>();
        List<Bits> docsWithFields = new ArrayList<>();
        for(CodecReader reader : readers) {
          values.add(Sorter.getOrWrapNumeric(reader, sortField));
          docsWithFields.add(DocValues.getDocsWithField(reader, sortField.getField()));
        }

        final Double missingValue;
        if (sortField.getMissingValue() != null) {
          missingValue = (Double) sortField.getMissingValue();
        } else {
          missingValue = 0.0;
        }

        return new CrossReaderComparator() {
          @Override
          public int compare(int readerIndexA, int docIDA, int readerIndexB, int docIDB) {
            double valueA;
            if (docsWithFields.get(readerIndexA).get(docIDA)) {
              valueA = Double.longBitsToDouble(values.get(readerIndexA).get(docIDA));
            } else {
              valueA = missingValue;
            }

            double valueB;
            if (docsWithFields.get(readerIndexB).get(docIDB)) {
              valueB = Double.longBitsToDouble(values.get(readerIndexB).get(docIDB));
            } else {
              valueB = missingValue;
            }
            return reverseMul * Double.compare(valueA, valueB);
          }
        };
      }

    case FLOAT:
      {
        List<NumericDocValues> values = new ArrayList<>();
        List<Bits> docsWithFields = new ArrayList<>();
        for(CodecReader reader : readers) {
          values.add(Sorter.getOrWrapNumeric(reader, sortField));
          docsWithFields.add(DocValues.getDocsWithField(reader, sortField.getField()));
        }

        final Float missingValue;
        if (sortField.getMissingValue() != null) {
          missingValue = (Float) sortField.getMissingValue();
        } else {
          missingValue = 0.0f;
        }

        return new CrossReaderComparator() {
          @Override
          public int compare(int readerIndexA, int docIDA, int readerIndexB, int docIDB) {
            float valueA;
            if (docsWithFields.get(readerIndexA).get(docIDA)) {
              valueA = Float.intBitsToFloat((int) values.get(readerIndexA).get(docIDA));
            } else {
              valueA = missingValue;
            }

            float valueB;
            if (docsWithFields.get(readerIndexB).get(docIDB)) {
              valueB = Float.intBitsToFloat((int) values.get(readerIndexB).get(docIDB));
            } else {
              valueB = missingValue;
            }
            return reverseMul * Float.compare(valueA, valueB);
          }
        };
      }

    default:
      throw new IllegalArgumentException("unhandled SortField.getType()=" + sortField.getType());
    }
  }
}
