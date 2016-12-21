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
package org.apache.lucene.codecs;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.MultiDocValues.OrdinalMap;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState; // javadocs
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.PackedInts;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** 
 * Abstract API that consumes numeric, binary and
 * sorted docvalues.  Concrete implementations of this
 * actually do "something" with the docvalues (write it into
 * the index in a specific format).
 * <p>
 * The lifecycle is:
 * <ol>
 *   <li>DocValuesConsumer is created by 
 *       {@link NormsFormat#normsConsumer(SegmentWriteState)}.
 *   <li>{@link #addNumericField}, {@link #addBinaryField},
 *       {@link #addSortedField}, {@link #addSortedSetField},
 *       or {@link #addSortedNumericField} are called for each Numeric,
 *       Binary, Sorted, SortedSet, or SortedNumeric docvalues field. 
 *       The API is a "pull" rather than "push", and the implementation 
 *       is free to iterate over the values multiple times 
 *       ({@link Iterable#iterator()}).
 *   <li>After all fields are added, the consumer is {@link #close}d.
 * </ol>
 *
 * @lucene.experimental
 */
public abstract class DocValuesConsumer implements Closeable {
  
  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected DocValuesConsumer() {}

  /**
   * Writes numeric docvalues for a field.
   * @param field field information
   * @param values Iterable of numeric values (one for each document). {@code null} indicates
   *               a missing value.
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException;    

  /**
   * Writes binary docvalues for a field.
   * @param field field information
   * @param values Iterable of binary values (one for each document). {@code null} indicates
   *               a missing value.
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addBinaryField(FieldInfo field, Iterable<BytesRef> values) throws IOException;

  /**
   * Writes pre-sorted binary docvalues for a field.
   * @param field field information
   * @param values Iterable of binary values in sorted order (deduplicated).
   * @param docToOrd Iterable of ordinals (one for each document). {@code -1} indicates
   *                 a missing value.
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException;
  
  /**
   * Writes pre-sorted numeric docvalues for a field
   * @param field field information
   * @param docToValueCount Iterable of the number of values for each document. A zero
   *                        count indicates a missing value.
   * @param values Iterable of numeric values in sorted order (not deduplicated).
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addSortedNumericField(FieldInfo field, Iterable<Number> docToValueCount, Iterable<Number> values) throws IOException;

  /**
   * Writes pre-sorted set docvalues for a field
   * @param field field information
   * @param values Iterable of binary values in sorted order (deduplicated).
   * @param docToOrdCount Iterable of the number of values for each document. A zero ordinal
   *                      count indicates a missing value.
   * @param ords Iterable of ordinal occurrences (docToOrdCount*maxDoc total).
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addSortedSetField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrdCount, Iterable<Number> ords) throws IOException;
  
  /** Merges in the fields from the readers in 
   *  <code>mergeState</code>. The default implementation 
   *  calls {@link #mergeNumericField}, {@link #mergeBinaryField},
   *  {@link #mergeSortedField}, {@link #mergeSortedSetField},
   *  or {@link #mergeSortedNumericField} for each field,
   *  depending on its type.
   *  Implementations can override this method 
   *  for more sophisticated merging (bulk-byte copying, etc). */
  public void merge(MergeState mergeState) throws IOException {
    for(DocValuesProducer docValuesProducer : mergeState.docValuesProducers) {
      if (docValuesProducer != null) {
        docValuesProducer.checkIntegrity();
      }
    }

    for (FieldInfo mergeFieldInfo : mergeState.mergeFieldInfos) {
      DocValuesType type = mergeFieldInfo.getDocValuesType();
      if (type != DocValuesType.NONE) {
        if (type == DocValuesType.NUMERIC) {
          List<NumericDocValues> toMerge = new ArrayList<>();
          List<Bits> docsWithField = new ArrayList<>();
          for (int i=0;i<mergeState.docValuesProducers.length;i++) {
            NumericDocValues values = null;
            Bits bits = null;
            DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
            if (docValuesProducer != null) {
              FieldInfo fieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
              if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.NUMERIC) {
                values = docValuesProducer.getNumeric(fieldInfo);
                bits = docValuesProducer.getDocsWithField(fieldInfo);
              }
            }
            if (values == null) {
              values = DocValues.emptyNumeric();
              bits = new Bits.MatchNoBits(mergeState.maxDocs[i]);
            }
            toMerge.add(values);
            docsWithField.add(bits);
          }
          mergeNumericField(mergeFieldInfo, mergeState, toMerge, docsWithField);
        } else if (type == DocValuesType.BINARY) {
          List<BinaryDocValues> toMerge = new ArrayList<>();
          List<Bits> docsWithField = new ArrayList<>();
          for (int i=0;i<mergeState.docValuesProducers.length;i++) {
            BinaryDocValues values = null;
            Bits bits = null;
            DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
            if (docValuesProducer != null) {
              FieldInfo fieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
              if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.BINARY) {
                values = docValuesProducer.getBinary(fieldInfo);
                bits = docValuesProducer.getDocsWithField(fieldInfo);
              }
            }
            if (values == null) {
              values = DocValues.emptyBinary();
              bits = new Bits.MatchNoBits(mergeState.maxDocs[i]);
            }
            toMerge.add(values);
            docsWithField.add(bits);
          }
          mergeBinaryField(mergeFieldInfo, mergeState, toMerge, docsWithField);
        } else if (type == DocValuesType.SORTED) {
          List<SortedDocValues> toMerge = new ArrayList<>();
          for (int i=0;i<mergeState.docValuesProducers.length;i++) {
            SortedDocValues values = null;
            DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
            if (docValuesProducer != null) {
              FieldInfo fieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
              if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.SORTED) {
                values = docValuesProducer.getSorted(fieldInfo);
              }
            }
            if (values == null) {
              values = DocValues.emptySorted();
            }
            toMerge.add(values);
          }
          mergeSortedField(mergeFieldInfo, mergeState, toMerge);
        } else if (type == DocValuesType.SORTED_SET) {
          List<SortedSetDocValues> toMerge = new ArrayList<>();
          for (int i=0;i<mergeState.docValuesProducers.length;i++) {
            SortedSetDocValues values = null;
            DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
            if (docValuesProducer != null) {
              FieldInfo fieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
              if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.SORTED_SET) {
                values = docValuesProducer.getSortedSet(fieldInfo);
              }
            }
            if (values == null) {
              values = DocValues.emptySortedSet();
            }
            toMerge.add(values);
          }
          mergeSortedSetField(mergeFieldInfo, mergeState, toMerge);
        } else if (type == DocValuesType.SORTED_NUMERIC) {
          List<SortedNumericDocValues> toMerge = new ArrayList<>();
          List<SortedNumericDocValues> toMerge2 = new ArrayList<>();
          for (int i=0;i<mergeState.docValuesProducers.length;i++) {
            SortedNumericDocValues values = null;
            SortedNumericDocValues values2 = null;
            DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
            if (docValuesProducer != null) {
              FieldInfo fieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
              if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.SORTED_NUMERIC) {
                values = docValuesProducer.getSortedNumeric(fieldInfo);
                values2 = docValuesProducer.getSortedNumeric(fieldInfo);
              }
            }
            if (values == null) {
              values = DocValues.emptySortedNumeric(mergeState.maxDocs[i]);
              values2 = values;
            }
            toMerge.add(values);
            toMerge2.add(values2);
          }
          mergeSortedNumericField(mergeFieldInfo, mergeState, toMerge, toMerge2);
        } else {
          throw new AssertionError("type=" + type);
        }
      }
    }
  }

  /** Tracks state of one numeric sub-reader that we are merging */
  private static class NumericDocValuesSub extends DocIDMerger.Sub {

    private final NumericDocValues values;
    private final Bits docsWithField;
    private int docID = -1;
    private final int maxDoc;

    public NumericDocValuesSub(MergeState.DocMap docMap, NumericDocValues values, Bits docsWithField, int maxDoc) {
      super(docMap);
      this.values = values;
      this.docsWithField = docsWithField;
      this.maxDoc = maxDoc;
    }

    @Override
    public int nextDoc() {
      docID++;
      if (docID == maxDoc) {
        return NO_MORE_DOCS;
      } else {
        return docID;
      }
    }
  }
  
  /**
   * Merges the numeric docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addNumericField}, passing
   * an Iterable that merges and filters deleted documents on the fly.
   */
  public void mergeNumericField(final FieldInfo fieldInfo, final MergeState mergeState, final List<NumericDocValues> toMerge, final List<Bits> docsWithField) throws IOException {
    addNumericField(fieldInfo,
                    new Iterable<Number>() {
                      @Override
                      public Iterator<Number> iterator() {

                        // We must make a new DocIDMerger for each iterator:
                        List<NumericDocValuesSub> subs = new ArrayList<>();
                        assert mergeState.docMaps.length == toMerge.size();
                        for(int i=0;i<toMerge.size();i++) {
                          subs.add(new NumericDocValuesSub(mergeState.docMaps[i], toMerge.get(i), docsWithField.get(i), mergeState.maxDocs[i]));
                        }

                        final DocIDMerger<NumericDocValuesSub> docIDMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);

                        return new Iterator<Number>() {
                          long nextValue;
                          boolean nextHasValue;
                          boolean nextIsSet;

                          @Override
                          public boolean hasNext() {
                            return nextIsSet || setNext();
                          }

                          @Override
                          public void remove() {
                            throw new UnsupportedOperationException();
                          }

                          @Override
                          public Number next() {
                            if (hasNext() == false) {
                              throw new NoSuchElementException();
                            }
                            assert nextIsSet;
                            nextIsSet = false;
                            return nextHasValue ? nextValue : null;
                          }

                          private boolean setNext() {
                            NumericDocValuesSub sub = docIDMerger.next();
                            if (sub == null) {
                              return false;
                            }
                            nextIsSet = true;
                            nextValue = sub.values.get(sub.docID);
                            nextHasValue = nextValue != 0 || sub.docsWithField.get(sub.docID);
                            return true;
                          }
                        };
                      }
                    });
  }
  
  /** Tracks state of one binary sub-reader that we are merging */
  private static class BinaryDocValuesSub extends DocIDMerger.Sub {

    private final BinaryDocValues values;
    private final Bits docsWithField;
    private int docID = -1;
    private final int maxDoc;

    public BinaryDocValuesSub(MergeState.DocMap docMap, BinaryDocValues values, Bits docsWithField, int maxDoc) {
      super(docMap);
      this.values = values;
      this.docsWithField = docsWithField;
      this.maxDoc = maxDoc;
    }

    @Override
    public int nextDoc() {
      docID++;
      if (docID == maxDoc) {
        return NO_MORE_DOCS;
      } else {
        return docID;
      }
    }
  }

  /**
   * Merges the binary docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addBinaryField}, passing
   * an Iterable that merges and filters deleted documents on the fly.
   */
  public void mergeBinaryField(FieldInfo fieldInfo, final MergeState mergeState, final List<BinaryDocValues> toMerge, final List<Bits> docsWithField) throws IOException {
    addBinaryField(fieldInfo,
                   new Iterable<BytesRef>() {
                     @Override
                     public Iterator<BytesRef> iterator() {

                       // We must make a new DocIDMerger for each iterator:
                       List<BinaryDocValuesSub> subs = new ArrayList<>();
                       assert mergeState.docMaps.length == toMerge.size();
                       for(int i=0;i<toMerge.size();i++) {
                         subs.add(new BinaryDocValuesSub(mergeState.docMaps[i], toMerge.get(i), docsWithField.get(i), mergeState.maxDocs[i]));
                       }

                       final DocIDMerger<BinaryDocValuesSub> docIDMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);

                       return new Iterator<BytesRef>() {
                         BytesRef nextValue;
                         BytesRef nextPointer; // points to null if missing, or nextValue
                         boolean nextIsSet;

                         @Override
                         public boolean hasNext() {
                           return nextIsSet || setNext();
                         }

                         @Override
                         public void remove() {
                           throw new UnsupportedOperationException();
                         }

                         @Override
                         public BytesRef next() {
                           if (hasNext() == false) {
                             throw new NoSuchElementException();
                           }
                           assert nextIsSet;
                           nextIsSet = false;
                           return nextPointer;
                         }

                         private boolean setNext() {
                           while (true) {
                              BinaryDocValuesSub sub = docIDMerger.next();
                              if (sub == null) {
                                return false;
                              }
                              nextIsSet = true;
                              if (sub.docsWithField.get(sub.docID)) {
                                nextPointer = nextValue = sub.values.get(sub.docID);
                              } else {
                                nextPointer = null;
                              }
                              return true;
                             }
                           }
                       };
                     }
                   });
  }

  /** Tracks state of one sorted numeric sub-reader that we are merging */
  private static class SortedNumericDocValuesSub extends DocIDMerger.Sub {

    private final SortedNumericDocValues values;
    private int docID = -1;
    private final int maxDoc;

    public SortedNumericDocValuesSub(MergeState.DocMap docMap, SortedNumericDocValues values, int maxDoc) {
      super(docMap);
      this.values = values;
      this.maxDoc = maxDoc;
    }

    @Override
    public int nextDoc() {
      docID++;
      if (docID == maxDoc) {
        return NO_MORE_DOCS;
      } else {
        values.setDocument(docID);
        return docID;
      }
    }

    @Override
    public String toString() {
      return "SortedNumericDocValuesSub values=" + values + " docID=" + docID + " mappedDocID=" + mappedDocID;
    }
  }

  /**
   * Merges the sorted docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addSortedNumericField}, passing
   * iterables that filter deleted documents.
   * <p>
   * We require two <code>toMerge</code> lists because we need to separately iterate the values for each segment concurrently.
   */
  public void mergeSortedNumericField(FieldInfo fieldInfo, final MergeState mergeState, List<SortedNumericDocValues> toMerge, List<SortedNumericDocValues> toMerge2) throws IOException {
    
    addSortedNumericField(fieldInfo,
        // doc -> value count
        new Iterable<Number>() {
          @Override
          public Iterator<Number> iterator() {

            // We must make a new DocIDMerger for each iterator:
            List<SortedNumericDocValuesSub> subs = new ArrayList<>();
            assert mergeState.docMaps.length == toMerge.size();
            for(int i=0;i<toMerge.size();i++) {
              subs.add(new SortedNumericDocValuesSub(mergeState.docMaps[i], toMerge.get(i), mergeState.maxDocs[i]));
            }

            final DocIDMerger<SortedNumericDocValuesSub> docIDMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);

            return new Iterator<Number>() {
              int nextValue;
              boolean nextIsSet;

              @Override
              public boolean hasNext() {
                return nextIsSet || setNext();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public Number next() {
                if (hasNext() == false) {
                  throw new NoSuchElementException();
                }
                assert nextIsSet;
                nextIsSet = false;
                return nextValue;
              }

              private boolean setNext() {
                while (true) {
                  SortedNumericDocValuesSub sub = docIDMerger.next();
                  if (sub == null) {
                    return false;
                  }
                  nextIsSet = true;
                  nextValue = sub.values.count();
                  return true;
                }
              }
            };
          }
        },
        // values
        new Iterable<Number>() {
          @Override
          public Iterator<Number> iterator() {
            // We must make a new DocIDMerger for each iterator:
            List<SortedNumericDocValuesSub> subs = new ArrayList<>();
            assert mergeState.docMaps.length == toMerge2.size();
            for(int i=0;i<toMerge2.size();i++) {
              subs.add(new SortedNumericDocValuesSub(mergeState.docMaps[i], toMerge2.get(i), mergeState.maxDocs[i]));
            }

            final DocIDMerger<SortedNumericDocValuesSub> docIDMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);

            return new Iterator<Number>() {
              long nextValue;
              boolean nextIsSet;
              int valueUpto;
              int valueLength;
              SortedNumericDocValuesSub current;

              @Override
              public boolean hasNext() {
                return nextIsSet || setNext();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public Number next() {
                if (hasNext() == false) {
                  throw new NoSuchElementException();
                }
                assert nextIsSet;
                nextIsSet = false;
                return nextValue;
              }

              private boolean setNext() {
                while (true) {
                  
                  if (valueUpto < valueLength) {
                    nextValue = current.values.valueAt(valueUpto);
                    valueUpto++;
                    nextIsSet = true;
                    return true;
                  }

                  current = docIDMerger.next();
                  if (current == null) {
                    return false;
                  }
                  valueUpto = 0;
                  valueLength = current.values.count();
                  continue;
                }
              }
            };
          }
        }
     );
  }

  /** Tracks state of one sorted sub-reader that we are merging */
  private static class SortedDocValuesSub extends DocIDMerger.Sub {

    private final SortedDocValues values;
    private int docID = -1;
    private final int maxDoc;
    private final LongValues map;

    public SortedDocValuesSub(MergeState.DocMap docMap, SortedDocValues values, int maxDoc, LongValues map) {
      super(docMap);
      this.values = values;
      this.maxDoc = maxDoc;
      this.map = map;
    }

    @Override
    public int nextDoc() {
      docID++;
      if (docID == maxDoc) {
        return NO_MORE_DOCS;
      } else {
        return docID;
      }
    }
  }

  /**
   * Merges the sorted docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addSortedField}, passing
   * an Iterable that merges ordinals and values and filters deleted documents .
   */
  public void mergeSortedField(FieldInfo fieldInfo, final MergeState mergeState, List<SortedDocValues> toMerge) throws IOException {
    final int numReaders = toMerge.size();
    final SortedDocValues dvs[] = toMerge.toArray(new SortedDocValues[numReaders]);
    
    // step 1: iterate thru each sub and mark terms still in use
    TermsEnum liveTerms[] = new TermsEnum[dvs.length];
    long[] weights = new long[liveTerms.length];
    for (int sub=0;sub<numReaders;sub++) {
      SortedDocValues dv = dvs[sub];
      Bits liveDocs = mergeState.liveDocs[sub];
      int maxDoc = mergeState.maxDocs[sub];
      if (liveDocs == null) {
        liveTerms[sub] = dv.termsEnum();
        weights[sub] = dv.getValueCount();
      } else {
        LongBitSet bitset = new LongBitSet(dv.getValueCount());
        for (int i = 0; i < maxDoc; i++) {
          if (liveDocs.get(i)) {
            int ord = dv.getOrd(i);
            if (ord >= 0) {
              bitset.set(ord);
            }
          }
        }
        liveTerms[sub] = new BitsFilteredTermsEnum(dv.termsEnum(), bitset);
        weights[sub] = bitset.cardinality();
      }
    }
    
    // step 2: create ordinal map (this conceptually does the "merging")
    final OrdinalMap map = OrdinalMap.build(this, liveTerms, weights, PackedInts.COMPACT);
    
    // step 3: add field
    addSortedField(fieldInfo,
        // ord -> value
        new Iterable<BytesRef>() {
          @Override
          public Iterator<BytesRef> iterator() {
            return new Iterator<BytesRef>() {
              int currentOrd;

              @Override
              public boolean hasNext() {
                return currentOrd < map.getValueCount();
              }

              @Override
              public BytesRef next() {
                if (hasNext() == false) {
                  throw new NoSuchElementException();
                }
                int segmentNumber = map.getFirstSegmentNumber(currentOrd);
                int segmentOrd = (int)map.getFirstSegmentOrd(currentOrd);
                final BytesRef term = dvs[segmentNumber].lookupOrd(segmentOrd);
                currentOrd++;
                return term;
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };
          }
        },
        // doc -> ord
        new Iterable<Number>() {
          @Override
          public Iterator<Number> iterator() {
            // We must make a new DocIDMerger for each iterator:
            List<SortedDocValuesSub> subs = new ArrayList<>();
            assert mergeState.docMaps.length == toMerge.size();
            for(int i=0;i<toMerge.size();i++) {
              subs.add(new SortedDocValuesSub(mergeState.docMaps[i], toMerge.get(i), mergeState.maxDocs[i], map.getGlobalOrds(i)));
            }

            final DocIDMerger<SortedDocValuesSub> docIDMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);

            return new Iterator<Number>() {
              int nextValue;
              boolean nextIsSet;

              @Override
              public boolean hasNext() {
                return nextIsSet || setNext();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public Number next() {
                if (hasNext() == false) {
                  throw new NoSuchElementException();
                }
                assert nextIsSet;
                nextIsSet = false;
                // TODO make a mutable number
                return nextValue;
              }

              private boolean setNext() {
                while (true) {
                  SortedDocValuesSub sub = docIDMerger.next();
                  if (sub == null) {
                    return false;
                  }

                  nextIsSet = true;
                  int segOrd = sub.values.getOrd(sub.docID);
                  nextValue = segOrd == -1 ? -1 : (int) sub.map.get(segOrd);
                  return true;
                }
              }
            };
          }
        }
    );
  }
  
  /** Tracks state of one sorted set sub-reader that we are merging */
  private static class SortedSetDocValuesSub extends DocIDMerger.Sub {

    private final SortedSetDocValues values;
    int docID = -1;
    private final int maxDoc;
    private final LongValues map;

    public SortedSetDocValuesSub(MergeState.DocMap docMap, SortedSetDocValues values, int maxDoc, LongValues map) {
      super(docMap);
      this.values = values;
      this.maxDoc = maxDoc;
      this.map = map;
    }

    @Override
    public int nextDoc() {
      docID++;
      if (docID == maxDoc) {
        return NO_MORE_DOCS;
      } else {
        return docID;
      }
    }

    @Override
    public String toString() {
      return "SortedSetDocValuesSub(docID=" + docID + " mappedDocID=" + mappedDocID + " values=" + values + ")";
    }
  }

  /**
   * Merges the sortedset docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addSortedSetField}, passing
   * an Iterable that merges ordinals and values and filters deleted documents .
   */
  public void mergeSortedSetField(FieldInfo fieldInfo, final MergeState mergeState, List<SortedSetDocValues> toMerge) throws IOException {

    // step 1: iterate thru each sub and mark terms still in use
    TermsEnum liveTerms[] = new TermsEnum[toMerge.size()];
    long[] weights = new long[liveTerms.length];
    for (int sub = 0; sub < liveTerms.length; sub++) {
      SortedSetDocValues dv = toMerge.get(sub);
      Bits liveDocs = mergeState.liveDocs[sub];
      int maxDoc = mergeState.maxDocs[sub];
      if (liveDocs == null) {
        liveTerms[sub] = dv.termsEnum();
        weights[sub] = dv.getValueCount();
      } else {
        LongBitSet bitset = new LongBitSet(dv.getValueCount());
        for (int i = 0; i < maxDoc; i++) {
          if (liveDocs.get(i)) {
            dv.setDocument(i);
            long ord;
            while ((ord = dv.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
              bitset.set(ord);
            }
          }
        }
        liveTerms[sub] = new BitsFilteredTermsEnum(dv.termsEnum(), bitset);
        weights[sub] = bitset.cardinality();
      }
    }
    
    // step 2: create ordinal map (this conceptually does the "merging")
    final OrdinalMap map = OrdinalMap.build(this, liveTerms, weights, PackedInts.COMPACT);
    
    // step 3: add field
    addSortedSetField(fieldInfo,
        // ord -> value
        new Iterable<BytesRef>() {
          @Override
          public Iterator<BytesRef> iterator() {
            return new Iterator<BytesRef>() {
              long currentOrd;

              @Override
              public boolean hasNext() {
                return currentOrd < map.getValueCount();
              }

              @Override
              public BytesRef next() {
                if (hasNext() == false) {
                  throw new NoSuchElementException();
                }
                int segmentNumber = map.getFirstSegmentNumber(currentOrd);
                long segmentOrd = map.getFirstSegmentOrd(currentOrd);
                final BytesRef term = toMerge.get(segmentNumber).lookupOrd(segmentOrd);
                currentOrd++;
                return term;
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };
          }
        },
        // doc -> ord count
        new Iterable<Number>() {
          @Override
          public Iterator<Number> iterator() {

            // We must make a new DocIDMerger for each iterator:
            List<SortedSetDocValuesSub> subs = new ArrayList<>();
            assert mergeState.docMaps.length == toMerge.size();
            for(int i=0;i<toMerge.size();i++) {
              subs.add(new SortedSetDocValuesSub(mergeState.docMaps[i], toMerge.get(i), mergeState.maxDocs[i], map.getGlobalOrds(i)));
            }

            final DocIDMerger<SortedSetDocValuesSub> docIDMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);

            return new Iterator<Number>() {
              int nextValue;
              boolean nextIsSet;

              @Override
              public boolean hasNext() {
                return nextIsSet || setNext();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public Number next() {
                if (hasNext() == false) {
                  throw new NoSuchElementException();
                }
                assert nextIsSet;
                nextIsSet = false;
                // TODO make a mutable number
                return nextValue;
              }

              private boolean setNext() {
                while (true) {
                  SortedSetDocValuesSub sub = docIDMerger.next();
                  if (sub == null) {
                    return false;
                  }
                  sub.values.setDocument(sub.docID);
                  nextValue = 0;
                  while (sub.values.nextOrd() != SortedSetDocValues.NO_MORE_ORDS) {
                    nextValue++;
                  }
                  //System.out.println("  doc " + sub + " -> ord count = " + nextValue);
                  nextIsSet = true;
                  return true;
                }
              }
            };
          }
        },
        // ords
        new Iterable<Number>() {
          @Override
          public Iterator<Number> iterator() {

            // We must make a new DocIDMerger for each iterator:
            List<SortedSetDocValuesSub> subs = new ArrayList<>();
            assert mergeState.docMaps.length == toMerge.size();
            for(int i=0;i<toMerge.size();i++) {
              subs.add(new SortedSetDocValuesSub(mergeState.docMaps[i], toMerge.get(i), mergeState.maxDocs[i], map.getGlobalOrds(i)));
            }

            final DocIDMerger<SortedSetDocValuesSub> docIDMerger = DocIDMerger.of(subs, mergeState.segmentInfo.getIndexSort() != null);

            return new Iterator<Number>() {
              long nextValue;
              boolean nextIsSet;
              long ords[] = new long[8];
              int ordUpto;
              int ordLength;

              @Override
              public boolean hasNext() {
                return nextIsSet || setNext();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public Number next() {
                if (hasNext() == false) {
                  throw new NoSuchElementException();
                }
                assert nextIsSet;
                nextIsSet = false;
                // TODO make a mutable number
                return nextValue;
              }

              private boolean setNext() {
                while (true) {
                  if (ordUpto < ordLength) {
                    nextValue = ords[ordUpto];
                    ordUpto++;
                    nextIsSet = true;
                    return true;
                  }

                  SortedSetDocValuesSub sub = docIDMerger.next();
                  if (sub == null) {
                    return false;
                  }
                  sub.values.setDocument(sub.docID);

                  ordUpto = ordLength = 0;
                  long ord;
                  while ((ord = sub.values.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
                    if (ordLength == ords.length) {
                      ords = ArrayUtil.grow(ords, ordLength+1);
                    }
                    ords[ordLength] = sub.map.get(ord);
                    ordLength++;
                  }
                  continue;
                }
              }
            };
          }
        }
     );
  }
  
  // TODO: seek-by-ord to nextSetBit
  static class BitsFilteredTermsEnum extends FilteredTermsEnum {
    final LongBitSet liveTerms;
    
    BitsFilteredTermsEnum(TermsEnum in, LongBitSet liveTerms) {
      super(in, false); // <-- not passing false here wasted about 3 hours of my time!!!!!!!!!!!!!
      assert liveTerms != null;
      this.liveTerms = liveTerms;
    }

    @Override
    protected AcceptStatus accept(BytesRef term) throws IOException {
      if (liveTerms.get(ord())) {
        return AcceptStatus.YES;
      } else {
        return AcceptStatus.NO;
      }
    }
  }
  
  /** Helper: returns true if the given docToValue count contains only at most one value */
  public static boolean isSingleValued(Iterable<Number> docToValueCount) {
    for (Number count : docToValueCount) {
      if (count.longValue() > 1) {
        return false;
      }
    }
    return true;
  }
  
  /** Helper: returns single-valued view, using {@code missingValue} when count is zero */
  public static Iterable<Number> singletonView(final Iterable<Number> docToValueCount, final Iterable<Number> values, final Number missingValue) {
    assert isSingleValued(docToValueCount);
    return new Iterable<Number>() {

      @Override
      public Iterator<Number> iterator() {
        final Iterator<Number> countIterator = docToValueCount.iterator();
        final Iterator<Number> valuesIterator = values.iterator();
        return new Iterator<Number>() {

          @Override
          public boolean hasNext() {
            return countIterator.hasNext();
          }

          @Override
          public Number next() {
            int count = countIterator.next().intValue();
            if (count == 0) {
              return missingValue;
            } else {
              return valuesIterator.next();
            }
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }
}
