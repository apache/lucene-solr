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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.packed.AppendingLongBuffer;

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

  static class SortedBytesMerger {

    public int numMergedTerms;

    final AppendingLongBuffer ordToReaderId = new AppendingLongBuffer();
    final List<SegmentState> segStates = new ArrayList<SegmentState>();

    private static class SegmentState {
      int segmentID;
      AtomicReader reader;
      FixedBitSet liveTerms;
      int ord = -1;
      SortedDocValues values;
      BytesRef scratch = new BytesRef();
      AppendingLongBuffer ordDeltas = new AppendingLongBuffer();

      // nocommit can we factor out the compressed fields
      // compression?  ie we have a good idea "roughly" what
      // the ord should be (linear projection) so we only
      // need to encode the delta from that ...:        
      int[] segOrdToMergedOrd;

      public BytesRef nextTerm() {
        while (ord < values.getValueCount()-1) {
          ord++;
          if (liveTerms == null || liveTerms.get(ord)) {
            values.lookupOrd(ord, scratch);
            return scratch;
          }
        }

        return null;
      }
    }

    private static class TermMergeQueue extends PriorityQueue<SegmentState> {
      public TermMergeQueue(int maxSize) {
        super(maxSize);
      }

      @Override
      protected boolean lessThan(SegmentState a, SegmentState b) {
        return a.scratch.compareTo(b.scratch) <= 0;
      }
    }

    public void merge(MergeState mergeState, List<SortedDocValues> toMerge) throws IOException {

      // First pass: mark "live" terms
      for (int readerIDX=0;readerIDX<toMerge.size();readerIDX++) {
        AtomicReader reader = mergeState.readers.get(readerIDX);      
        int maxDoc = reader.maxDoc();

        SegmentState state = new SegmentState();
        state.segmentID = readerIDX;
        state.reader = reader;
        state.values = toMerge.get(readerIDX);

        segStates.add(state);
        assert state.values.getValueCount() < Integer.MAX_VALUE;
        if (reader.hasDeletions()) {
          state.liveTerms = new FixedBitSet(state.values.getValueCount());
          Bits liveDocs = reader.getLiveDocs();
          assert liveDocs != null;
          for(int docID=0;docID<maxDoc;docID++) {
            if (liveDocs.get(docID)) {
              state.liveTerms.set(state.values.getOrd(docID));
            }
          }
        }

        // nocommit we can unload the bits to disk to reduce
        // transient ram spike...
      }

      // Second pass: merge only the live terms

      TermMergeQueue q = new TermMergeQueue(segStates.size());
      for(SegmentState segState : segStates) {
        if (segState.nextTerm() != null) {

          // nocommit we could defer this to 3rd pass (and
          // reduce transient RAM spike) but then
          // we'd spend more effort computing the mapping...:
          segState.segOrdToMergedOrd = new int[segState.values.getValueCount()];
          q.add(segState);
        }
      }

      int lastOrds[] = new int[segStates.size()];
      BytesRef lastTerm = null;
      int ord = 0;
      while (q.size() != 0) {
        SegmentState top = q.top();
        if (lastTerm == null || !lastTerm.equals(top.scratch)) {
          // a new unique term: record its segment ID / sourceOrd pair
          int readerId = top.segmentID;
          ordToReaderId.add(readerId);

          int sourceOrd = top.ord;             
          int delta = sourceOrd - lastOrds[readerId];
          lastOrds[readerId] = sourceOrd;
          top.ordDeltas.add(delta);
          
          lastTerm = BytesRef.deepCopyOf(top.scratch);
          ord++;
        }

        top.segOrdToMergedOrd[top.ord] = ord-1;
        if (top.nextTerm() == null) {
          q.pop();
        } else {
          q.updateTop();
        }
      }

      numMergedTerms = ord;
      // clear our bitsets for GC: we dont need them anymore (e.g. while flushing merged stuff to codec)
      for (SegmentState state : segStates) {
        state.liveTerms = null;
      }
    }

    /*
    public void finish(SortedDocValuesConsumer consumer) throws IOException {

      // Third pass: write merged result
      for(BytesRef term : mergedTerms) {
        consumer.addValue(term);
      }

      for(SegmentState segState : segStates) {
        Bits liveDocs = segState.reader.getLiveDocs();
        int maxDoc = segState.reader.maxDoc();
        for(int docID=0;docID<maxDoc;docID++) {
          if (liveDocs == null || liveDocs.get(docID)) {
            int segOrd = segState.values.getOrd(docID);
            int mergedOrd = segState.segOrdToMergedOrd[segOrd];
            consumer.addDoc(mergedOrd);
          }
        }
      }
    }
    */
  }

  /**
   * Merges the sorted docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addSortedField}, passing
   * an Iterable that merges ordinals and values and filters deleted documents .
   */
  public void mergeSortedField(FieldInfo fieldInfo, final MergeState mergeState, List<SortedDocValues> toMerge) throws IOException {
    final SortedBytesMerger merger = new SortedBytesMerger();

    // Does the heavy lifting to merge sort all "live" ords:
    merger.merge(mergeState, toMerge);

    addSortedField(fieldInfo,

                   // ord -> value
                   new Iterable<BytesRef>() {
                     @Override
                     public Iterator<BytesRef> iterator() {
                       // for each next(), tells us what reader to go to
                       final AppendingLongBuffer.Iterator readerIDs = merger.ordToReaderId.iterator();
                       // for each next(), gives us the original ord
                       final AppendingLongBuffer.Iterator ordDeltas[] = new AppendingLongBuffer.Iterator[merger.segStates.size()];
                       final int lastOrds[] = new int[ordDeltas.length];
                       
                       for (int i = 0; i < ordDeltas.length; i++) {
                         ordDeltas[i] = merger.segStates.get(i).ordDeltas.iterator();
                       }

                       final BytesRef scratch = new BytesRef();
                       
                       return new Iterator<BytesRef>() {
                         int ordUpto;

                         @Override
                         public boolean hasNext() {
                           return ordUpto < merger.numMergedTerms;
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
                           int readerID = (int) readerIDs.next();
                           int ord = lastOrds[readerID] + (int) ordDeltas[readerID].next();
                           merger.segStates.get(readerID).values.lookupOrd(ord, scratch);
                           lastOrds[readerID] = ord;
                           ordUpto++;
                           return scratch;
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
                          SortedBytesMerger.SegmentState currentReader;
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
                            // nocommit make a mutable number
                            return nextValue;
                          }

                          private boolean setNext() {
                            while (true) {
                              if (readerUpto == merger.segStates.size()) {
                                return false;
                              }

                              if (currentReader == null || docIDUpto == currentReader.reader.maxDoc()) {
                                readerUpto++;
                                if (readerUpto < merger.segStates.size()) {
                                  currentReader = merger.segStates.get(readerUpto);
                                  currentLiveDocs = currentReader.reader.getLiveDocs();
                                }
                                docIDUpto = 0;
                                continue;
                              }

                              if (currentLiveDocs == null || currentLiveDocs.get(docIDUpto)) {
                                nextIsSet = true;
                                int segOrd = currentReader.values.getOrd(docIDUpto);
                                nextValue = currentReader.segOrdToMergedOrd[segOrd];
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
}
