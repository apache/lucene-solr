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
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

@SuppressWarnings({"unchecked","rawtypes"})
final class MultiSorter {
  
  /** Does a merge sort of the leaves of the incoming reader, returning {@link DocMap} to map each leaf's
   *  documents into the merged segment.  The documents for each incoming leaf reader must already be sorted by the same sort!
   *  Returns null if the merge sort is not needed (segments are already in index sort order).
   **/
  static MergeState.DocMap[] sort(Sort sort, List<CodecReader> readers) throws IOException {

    // TODO: optimize if only 1 reader is incoming, though that's a rare case

    SortField fields[] = sort.getSort();
    final ComparableProvider[][] comparables = new ComparableProvider[fields.length][];
    for(int i=0;i<fields.length;i++) {
      comparables[i] = getComparableProviders(readers, fields[i]);
    }

    int leafCount = readers.size();

    PriorityQueue<LeafAndDocID> queue = new PriorityQueue<LeafAndDocID>(leafCount) {
        @Override
        public boolean lessThan(LeafAndDocID a, LeafAndDocID b) {
          for(int i=0;i<comparables.length;i++) {
            int cmp = a.values[i].compareTo(b.values[i]);
            if (cmp != 0) {
              return cmp < 0;
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
        leaf.values[j] = comparables[j][i].getComparable(leaf.docID);
        assert leaf.values[j] != null;
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
          top.values[j] = comparables[j][top.readerIndex].getComparable(top.docID);
          assert top.values[j] != null;
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
    final Comparable[] values;
    int docID;

    public LeafAndDocID(int readerIndex, Bits liveDocs, int maxDoc, int numComparables) {
      this.readerIndex = readerIndex;
      this.liveDocs = liveDocs;
      this.maxDoc = maxDoc;
      this.values = new Comparable[numComparables];
    }
  }

  /** Returns an object for this docID whose .compareTo represents the requested {@link SortField} sort order. */
  private interface ComparableProvider {
    public Comparable getComparable(int docID) throws IOException;
  }

  /** Returns {@code ComparableProvider}s for the provided readers to represent the requested {@link SortField} sort order. */
  private static ComparableProvider[] getComparableProviders(List<CodecReader> readers, SortField sortField) throws IOException {

    ComparableProvider[] providers = new ComparableProvider[readers.size()];
    final int reverseMul = sortField.getReverse() ? -1 : 1;
    final SortField.Type sortType = Sorter.getSortFieldType(sortField);

    switch(sortType) {

    case STRING:
      {
        // this uses the efficient segment-local ordinal map:
        final SortedDocValues[] values = new SortedDocValues[readers.size()];
        for(int i=0;i<readers.size();i++) {
          final SortedDocValues sorted = Sorter.getOrWrapSorted(readers.get(i), sortField);
          values[i] = sorted;
        }
        MultiDocValues.OrdinalMap ordinalMap = MultiDocValues.OrdinalMap.build(null, values, PackedInts.DEFAULT);
        final int missingOrd;
        if (sortField.getMissingValue() == SortField.STRING_LAST) {
          missingOrd = sortField.getReverse() ? Integer.MIN_VALUE : Integer.MAX_VALUE;
        } else {
          missingOrd = sortField.getReverse() ? Integer.MAX_VALUE : Integer.MIN_VALUE;
        }

        for(int readerIndex=0;readerIndex<readers.size();readerIndex++) {
          final SortedDocValues readerValues = values[readerIndex];
          final LongValues globalOrds = ordinalMap.getGlobalOrds(readerIndex);
          providers[readerIndex] = new ComparableProvider() {
              // used only by assert:
              int lastDocID = -1;
              private boolean docsInOrder(int docID) {
                if (docID < lastDocID) {
                  throw new AssertionError("docs must be sent in order, but lastDocID=" + lastDocID + " vs docID=" + docID);
                }
                lastDocID = docID;
                return true;
              }
              
              @Override
              public Comparable getComparable(int docID) throws IOException {
                assert docsInOrder(docID);
                int readerDocID = readerValues.docID();
                if (readerDocID < docID) {
                  readerDocID = readerValues.advance(docID);
                }
                if (readerDocID == docID) {
                  // translate segment's ord to global ord space:
                  return reverseMul * (int) globalOrds.get(readerValues.ordValue());
                } else {
                  return missingOrd;
                }
              }
            };
        }
      }
      break;

    case LONG:
      {
        final Long missingValue;
        if (sortField.getMissingValue() != null) {
          missingValue = (Long) sortField.getMissingValue();
        } else {
          missingValue = 0L;
        }

        for(int readerIndex=0;readerIndex<readers.size();readerIndex++) {
          final NumericDocValues values = Sorter.getOrWrapNumeric(readers.get(readerIndex), sortField);

          providers[readerIndex] = new ComparableProvider() {
              // used only by assert:
              int lastDocID = -1;
              private boolean docsInOrder(int docID) {
                if (docID < lastDocID) {
                  throw new AssertionError("docs must be sent in order, but lastDocID=" + lastDocID + " vs docID=" + docID);
                }
                lastDocID = docID;
                return true;
              }
              
              @Override
              public Comparable getComparable(int docID) throws IOException {
                assert docsInOrder(docID);
                int readerDocID = values.docID();
                if (readerDocID < docID) {
                  readerDocID = values.advance(docID);
                }
                if (readerDocID == docID) {
                  return reverseMul * values.longValue();
                } else {
                  return reverseMul * missingValue;
                }
              }
            };
        }
      }
      break;

    case INT:
      {
        final Integer missingValue;
        if (sortField.getMissingValue() != null) {
          missingValue = (Integer) sortField.getMissingValue();
        } else {
          missingValue = 0;
        }

        for(int readerIndex=0;readerIndex<readers.size();readerIndex++) {
          final NumericDocValues values = Sorter.getOrWrapNumeric(readers.get(readerIndex), sortField);

          providers[readerIndex] = new ComparableProvider() {
              // used only by assert:
              int lastDocID = -1;
              private boolean docsInOrder(int docID) {
                if (docID < lastDocID) {
                  throw new AssertionError("docs must be sent in order, but lastDocID=" + lastDocID + " vs docID=" + docID);
                }
                lastDocID = docID;
                return true;
              }
              
              @Override
              public Comparable getComparable(int docID) throws IOException {
                assert docsInOrder(docID);
                int readerDocID = values.docID();
                if (readerDocID < docID) {
                  readerDocID = values.advance(docID);
                }
                if (readerDocID == docID) {
                  return reverseMul * (int) values.longValue();
                } else {
                  return reverseMul * missingValue;
                }
              }
            };
        }
      }
      break;

    case DOUBLE:
      {
        final Double missingValue;
        if (sortField.getMissingValue() != null) {
          missingValue = (Double) sortField.getMissingValue();
        } else {
          missingValue = 0.0;
        }

        for(int readerIndex=0;readerIndex<readers.size();readerIndex++) {
          final NumericDocValues values = Sorter.getOrWrapNumeric(readers.get(readerIndex), sortField);

          providers[readerIndex] = new ComparableProvider() {
              // used only by assert:
              int lastDocID = -1;
              private boolean docsInOrder(int docID) {
                if (docID < lastDocID) {
                  throw new AssertionError("docs must be sent in order, but lastDocID=" + lastDocID + " vs docID=" + docID);
                }
                lastDocID = docID;
                return true;
              }
              
              @Override
              public Comparable getComparable(int docID) throws IOException {
                assert docsInOrder(docID);
                int readerDocID = values.docID();
                if (readerDocID < docID) {
                  readerDocID = values.advance(docID);
                }
                if (readerDocID == docID) {
                  return reverseMul * Double.longBitsToDouble(values.longValue());
                } else {
                  return reverseMul * missingValue;
                }
              }
            };
        }
      }
      break;

    case FLOAT:
      {
        final Float missingValue;
        if (sortField.getMissingValue() != null) {
          missingValue = (Float) sortField.getMissingValue();
        } else {
          missingValue = 0.0f;
        }

        for(int readerIndex=0;readerIndex<readers.size();readerIndex++) {
          final NumericDocValues values = Sorter.getOrWrapNumeric(readers.get(readerIndex), sortField);

          providers[readerIndex] = new ComparableProvider() {
              // used only by assert:
              int lastDocID = -1;
              private boolean docsInOrder(int docID) {
                if (docID < lastDocID) {
                  throw new AssertionError("docs must be sent in order, but lastDocID=" + lastDocID + " vs docID=" + docID);
                }
                lastDocID = docID;
                return true;
              }
              
              @Override
              public Comparable getComparable(int docID) throws IOException {
                assert docsInOrder(docID);
                int readerDocID = values.docID();
                if (readerDocID < docID) {
                  readerDocID = values.advance(docID);
                }
                if (readerDocID == docID) {
                  return reverseMul * Float.intBitsToFloat((int) values.longValue());
                } else {
                  return reverseMul * missingValue;
                }
              }
            };
        }
      }
      break;

    default:
      throw new IllegalArgumentException("unhandled SortField.getType()=" + sortField.getType());
    }

    return providers;
  }
}
