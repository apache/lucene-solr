package org.apache.lucene.codecs;

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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.MultiDocValues.OrdinalMap;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;

/** 
 * Abstract API that consumes numeric, binary and
 * sorted docvalues.  Concrete implementations of this
 * actually do "something" with the docvalues (write it into
 * the index in a specific format).
 * <p>
 * The lifecycle is:
 * <ol>
 *   <li>DocValuesConsumer is created by 
 *       {@link DocValuesFormat#fieldsConsumer(SegmentWriteState)} or
 *       {@link NormsFormat#normsConsumer(SegmentWriteState)}.
 *   <li>{@link #addNumericField}, {@link #addBinaryField},
 *       or {@link #addSortedField} are called for each Numeric,
 *       Binary, or Sorted docvalues field. The API is a "pull" rather
 *       than "push", and the implementation is free to iterate over the 
 *       values multiple times ({@link Iterable#iterator()}).
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
   * @param values Iterable of numeric values (one for each document).
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException;    

  /**
   * Writes binary docvalues for a field.
   * @param field field information
   * @param values Iterable of binary values (one for each document).
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addBinaryField(FieldInfo field, Iterable<BytesRef> values) throws IOException;

  /**
   * Writes pre-sorted binary docvalues for a field.
   * @param field field information
   * @param values Iterable of binary values in sorted order (deduplicated).
   * @param docToOrd Iterable of ordinals (one for each document).
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException;

  /**
   * Writes pre-sorted set docvalues for a field
   * @param field field information
   * @param values Iterable of binary values in sorted order (deduplicated).
   * @param docToOrdCount Iterable of the number of values for each document. 
   * @param ords Iterable of ordinal occurrences (docToOrdCount*maxDoc total).
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addSortedSetField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrdCount, Iterable<Number> ords) throws IOException;
  
  /**
   * Merges the numeric docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addNumericField}, passing
   * an Iterable that merges and filters deleted documents on the fly.
   */
  public void mergeNumericField(FieldInfo fieldInfo, final MergeState mergeState, final List<NumericDocValues> toMerge) throws IOException {

    addNumericField(fieldInfo,
                    new Iterable<Number>() {
                      @Override
                      public Iterator<Number> iterator() {
                        return new Iterator<Number>() {
                          int readerUpto = -1;
                          int docIDUpto;
                          long nextValue;
                          AtomicReader currentReader;
                          NumericDocValues currentValues;
                          Bits currentLiveDocs;
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
                            if (!hasNext()) {
                              throw new NoSuchElementException();
                            }
                            assert nextIsSet;
                            nextIsSet = false;
                            // TODO: make a mutable number
                            return nextValue;
                          }

                          private boolean setNext() {
                            while (true) {
                              if (readerUpto == toMerge.size()) {
                                return false;
                              }

                              if (currentReader == null || docIDUpto == currentReader.maxDoc()) {
                                readerUpto++;
                                if (readerUpto < toMerge.size()) {
                                  currentReader = mergeState.readers.get(readerUpto);
                                  currentValues = toMerge.get(readerUpto);
                                  currentLiveDocs = currentReader.getLiveDocs();
                                }
                                docIDUpto = 0;
                                continue;
                              }

                              if (currentLiveDocs == null || currentLiveDocs.get(docIDUpto)) {
                                nextIsSet = true;
                                nextValue = currentValues.get(docIDUpto);
                                docIDUpto++;
                                return true;
                              }

                              docIDUpto++;
                            }
                          }
                        };
                      }
                    });
  }
  
  /**
   * Merges the binary docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addBinaryField}, passing
   * an Iterable that merges and filters deleted documents on the fly.
   */
  public void mergeBinaryField(FieldInfo fieldInfo, final MergeState mergeState, final List<BinaryDocValues> toMerge) throws IOException {

    addBinaryField(fieldInfo,
                   new Iterable<BytesRef>() {
                     @Override
                     public Iterator<BytesRef> iterator() {
                       return new Iterator<BytesRef>() {
                         int readerUpto = -1;
                         int docIDUpto;
                         BytesRef nextValue = new BytesRef();
                         AtomicReader currentReader;
                         BinaryDocValues currentValues;
                         Bits currentLiveDocs;
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
                           if (!hasNext()) {
                             throw new NoSuchElementException();
                           }
                           assert nextIsSet;
                           nextIsSet = false;
                           // TODO: make a mutable number
                           return nextValue;
                         }

                         private boolean setNext() {
                           while (true) {
                             if (readerUpto == toMerge.size()) {
                               return false;
                             }

                             if (currentReader == null || docIDUpto == currentReader.maxDoc()) {
                               readerUpto++;
                               if (readerUpto < toMerge.size()) {
                                 currentReader = mergeState.readers.get(readerUpto);
                                 currentValues = toMerge.get(readerUpto);
                                 currentLiveDocs = currentReader.getLiveDocs();
                               }
                               docIDUpto = 0;
                               continue;
                             }

                             if (currentLiveDocs == null || currentLiveDocs.get(docIDUpto)) {
                               nextIsSet = true;
                               currentValues.get(docIDUpto, nextValue);
                               docIDUpto++;
                               return true;
                             }

                             docIDUpto++;
                           }
                         }
                       };
                     }
                   });
  }


  /**
   * Merges the sorted docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addSortedField}, passing
   * an Iterable that merges ordinals and values and filters deleted documents .
   */
  public void mergeSortedField(FieldInfo fieldInfo, final MergeState mergeState, List<SortedDocValues> toMerge) throws IOException {
    final AtomicReader readers[] = mergeState.readers.toArray(new AtomicReader[toMerge.size()]);
    final SortedDocValues dvs[] = toMerge.toArray(new SortedDocValues[toMerge.size()]);
    
    // step 1: iterate thru each sub and mark terms still in use
    TermsEnum liveTerms[] = new TermsEnum[dvs.length];
    for (int sub = 0; sub < liveTerms.length; sub++) {
      AtomicReader reader = readers[sub];
      SortedDocValues dv = dvs[sub];
      Bits liveDocs = reader.getLiveDocs();
      if (liveDocs == null) {
        liveTerms[sub] = dv.termsEnum();
      } else {
        OpenBitSet bitset = new OpenBitSet(dv.getValueCount());
        for (int i = 0; i < reader.maxDoc(); i++) {
          if (liveDocs.get(i)) {
            bitset.set(dv.getOrd(i));
          }
        }
        liveTerms[sub] = new BitsFilteredTermsEnum(dv.termsEnum(), bitset);
      }
    }
    
    // step 2: create ordinal map (this conceptually does the "merging")
    final OrdinalMap map = new OrdinalMap(this, liveTerms);
    
    // step 3: add field
    addSortedField(fieldInfo,
        // ord -> value
        new Iterable<BytesRef>() {
          @Override
          public Iterator<BytesRef> iterator() {
            return new Iterator<BytesRef>() {
              final BytesRef scratch = new BytesRef();
              int currentOrd;

              @Override
              public boolean hasNext() {
                return currentOrd < map.getValueCount();
              }

              @Override
              public BytesRef next() {
                if (!hasNext()) {
                  throw new NoSuchElementException();
                }
                int segmentNumber = map.getSegmentNumber(currentOrd);
                int segmentOrd = (int)map.getSegmentOrd(segmentNumber, currentOrd);
                dvs[segmentNumber].lookupOrd(segmentOrd, scratch);
                currentOrd++;
                return scratch;
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
            return new Iterator<Number>() {
              int readerUpto = -1;
              int docIDUpto;
              int nextValue;
              AtomicReader currentReader;
              Bits currentLiveDocs;
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
                if (!hasNext()) {
                  throw new NoSuchElementException();
                }
                assert nextIsSet;
                nextIsSet = false;
                // TODO make a mutable number
                return nextValue;
              }

              private boolean setNext() {
                while (true) {
                  if (readerUpto == readers.length) {
                    return false;
                  }

                  if (currentReader == null || docIDUpto == currentReader.maxDoc()) {
                    readerUpto++;
                    if (readerUpto < readers.length) {
                      currentReader = readers[readerUpto];
                      currentLiveDocs = currentReader.getLiveDocs();
                    }
                    docIDUpto = 0;
                    continue;
                  }

                  if (currentLiveDocs == null || currentLiveDocs.get(docIDUpto)) {
                    nextIsSet = true;
                    int segOrd = dvs[readerUpto].getOrd(docIDUpto);
                    nextValue = (int) map.getGlobalOrd(readerUpto, segOrd);
                    docIDUpto++;
                    return true;
                  }

                  docIDUpto++;
                }
              }
            };
          }
        }
    );
  }
  
  /**
   * Merges the sortedset docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addSortedSetField}, passing
   * an Iterable that merges ordinals and values and filters deleted documents .
   */
  public void mergeSortedSetField(FieldInfo fieldInfo, final MergeState mergeState, List<SortedSetDocValues> toMerge) throws IOException {
    final AtomicReader readers[] = mergeState.readers.toArray(new AtomicReader[toMerge.size()]);
    final SortedSetDocValues dvs[] = toMerge.toArray(new SortedSetDocValues[toMerge.size()]);
    
    // step 1: iterate thru each sub and mark terms still in use
    TermsEnum liveTerms[] = new TermsEnum[dvs.length];
    for (int sub = 0; sub < liveTerms.length; sub++) {
      AtomicReader reader = readers[sub];
      SortedSetDocValues dv = dvs[sub];
      Bits liveDocs = reader.getLiveDocs();
      if (liveDocs == null) {
        liveTerms[sub] = dv.termsEnum();
      } else {
        OpenBitSet bitset = new OpenBitSet(dv.getValueCount());
        for (int i = 0; i < reader.maxDoc(); i++) {
          if (liveDocs.get(i)) {
            dv.setDocument(i);
            long ord;
            while ((ord = dv.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
              bitset.set(ord);
            }
          }
        }
        liveTerms[sub] = new BitsFilteredTermsEnum(dv.termsEnum(), bitset);
      }
    }
    
    // step 2: create ordinal map (this conceptually does the "merging")
    final OrdinalMap map = new OrdinalMap(this, liveTerms);
    
    // step 3: add field
    addSortedSetField(fieldInfo,
        // ord -> value
        new Iterable<BytesRef>() {
          @Override
          public Iterator<BytesRef> iterator() {
            return new Iterator<BytesRef>() {
              final BytesRef scratch = new BytesRef();
              long currentOrd;

              @Override
              public boolean hasNext() {
                return currentOrd < map.getValueCount();
              }

              @Override
              public BytesRef next() {
                if (!hasNext()) {
                  throw new NoSuchElementException();
                }
                int segmentNumber = map.getSegmentNumber(currentOrd);
                long segmentOrd = map.getSegmentOrd(segmentNumber, currentOrd);
                dvs[segmentNumber].lookupOrd(segmentOrd, scratch);
                currentOrd++;
                return scratch;
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
            return new Iterator<Number>() {
              int readerUpto = -1;
              int docIDUpto;
              int nextValue;
              AtomicReader currentReader;
              Bits currentLiveDocs;
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
                if (!hasNext()) {
                  throw new NoSuchElementException();
                }
                assert nextIsSet;
                nextIsSet = false;
                // TODO make a mutable number
                return nextValue;
              }

              private boolean setNext() {
                while (true) {
                  if (readerUpto == readers.length) {
                    return false;
                  }

                  if (currentReader == null || docIDUpto == currentReader.maxDoc()) {
                    readerUpto++;
                    if (readerUpto < readers.length) {
                      currentReader = readers[readerUpto];
                      currentLiveDocs = currentReader.getLiveDocs();
                    }
                    docIDUpto = 0;
                    continue;
                  }

                  if (currentLiveDocs == null || currentLiveDocs.get(docIDUpto)) {
                    nextIsSet = true;
                    SortedSetDocValues dv = dvs[readerUpto];
                    dv.setDocument(docIDUpto);
                    nextValue = 0;
                    while (dv.nextOrd() != SortedSetDocValues.NO_MORE_ORDS) {
                      nextValue++;
                    }
                    docIDUpto++;
                    return true;
                  }

                  docIDUpto++;
                }
              }
            };
          }
        },
        // ords
        new Iterable<Number>() {
          @Override
          public Iterator<Number> iterator() {
            return new Iterator<Number>() {
              int readerUpto = -1;
              int docIDUpto;
              long nextValue;
              AtomicReader currentReader;
              Bits currentLiveDocs;
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
                if (!hasNext()) {
                  throw new NoSuchElementException();
                }
                assert nextIsSet;
                nextIsSet = false;
                // TODO make a mutable number
                return nextValue;
              }

              private boolean setNext() {
                while (true) {
                  if (readerUpto == readers.length) {
                    return false;
                  }
                  
                  if (ordUpto < ordLength) {
                    nextValue = ords[ordUpto];
                    ordUpto++;
                    nextIsSet = true;
                    return true;
                  }

                  if (currentReader == null || docIDUpto == currentReader.maxDoc()) {
                    readerUpto++;
                    if (readerUpto < readers.length) {
                      currentReader = readers[readerUpto];
                      currentLiveDocs = currentReader.getLiveDocs();
                    }
                    docIDUpto = 0;
                    continue;
                  }
                  
                  if (currentLiveDocs == null || currentLiveDocs.get(docIDUpto)) {
                    assert docIDUpto < currentReader.maxDoc();
                    SortedSetDocValues dv = dvs[readerUpto];
                    dv.setDocument(docIDUpto);
                    ordUpto = ordLength = 0;
                    long ord;
                    while ((ord = dv.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
                      if (ordLength == ords.length) {
                        ords = ArrayUtil.grow(ords, ordLength+1);
                      }
                      ords[ordLength] = map.getGlobalOrd(readerUpto, ord);
                      ordLength++;
                    }
                    docIDUpto++;
                    continue;
                  }

                  docIDUpto++;
                }
              }
            };
          }
        }
     );
  }
  
  // TODO: seek-by-ord to nextSetBit
  static class BitsFilteredTermsEnum extends FilteredTermsEnum {
    final OpenBitSet liveTerms;
    
    BitsFilteredTermsEnum(TermsEnum in, OpenBitSet liveTerms) {
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
}
