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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.document.ReferenceDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.ReferenceDocValuesWriter;
import org.apache.lucene.index.SegmentWriteState; // javadocs
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.VectorDocValues;
import org.apache.lucene.index.VectorDocValuesWriter;
import org.apache.lucene.search.GraphSearch;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
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
   * @param valuesProducer Numeric values to write.
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException;    

  /**
   * Writes binary docvalues for a field.
   * @param field field information
   * @param valuesProducer Binary values to write.
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException;

  /**
   * Writes pre-sorted binary docvalues for a field.
   * @param field field information
   * @param valuesProducer produces the values and ordinals to write
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException;
  
  /**
   * Writes pre-sorted numeric docvalues for a field
   * @param field field information
   * @param valuesProducer produces the values to write
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException;

  /**
   * Writes pre-sorted set docvalues for a field
   * @param field field information
   * @param valuesProducer produces the values to write
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException;
  
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
          mergeNumericField(mergeFieldInfo, mergeState);
        } else if (type == DocValuesType.BINARY) {
          mergeBinaryField(mergeFieldInfo, mergeState);
        } else if (type == DocValuesType.SORTED) {
          mergeSortedField(mergeFieldInfo, mergeState);
        } else if (type == DocValuesType.SORTED_SET) {
          mergeSortedSetField(mergeFieldInfo, mergeState);
        } else if (type == DocValuesType.SORTED_NUMERIC) {
          String refType = mergeFieldInfo.getAttribute(ReferenceDocValuesField.REFTYPE_ATTR);
          if ("knn-graph".equals(refType)) {
            mergeReferenceField(mergeFieldInfo, mergeState);
          } else {
            mergeSortedNumericField(mergeFieldInfo, mergeState);
          }
        } else {
          throw new AssertionError("type=" + type);
        }
      }
    }
  }

  /** Tracks state of one numeric sub-reader that we are merging */
  private static class NumericDocValuesSub extends DocIDMerger.Sub {

    final NumericDocValues values;

    public NumericDocValuesSub(MergeState.DocMap docMap, NumericDocValues values) {
      super(docMap);
      this.values = values;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }
  
  /**
   * Merges the numeric docvalues from <code>MergeState</code>.
   * <p>
   * The default implementation calls {@link #addNumericField}, passing
   * a DocValuesProducer that merges and filters deleted documents on the fly.
   */
  public void mergeNumericField(final FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {
    addNumericField(mergeFieldInfo,
                    new EmptyDocValuesProducer() {
                      @Override
                      public NumericDocValues getNumeric(FieldInfo fieldInfo) throws IOException {
                        if (fieldInfo != mergeFieldInfo) {
                          throw new IllegalArgumentException("wrong fieldInfo");
                        }

                        List<NumericDocValuesSub> subs = new ArrayList<>();
                        assert mergeState.docMaps.length == mergeState.docValuesProducers.length;
                        long cost = 0;
                        for (int i=0;i<mergeState.docValuesProducers.length;i++) {
                          NumericDocValues values = null;
                          DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
                          if (docValuesProducer != null) {
                            FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
                            if (readerFieldInfo != null && readerFieldInfo.getDocValuesType() == DocValuesType.NUMERIC) {
                              values = docValuesProducer.getNumeric(readerFieldInfo);
                            }
                          }
                          if (values != null) {
                            cost += values.cost();
                            subs.add(new NumericDocValuesSub(mergeState.docMaps[i], values));
                          }
                        }

                        final DocIDMerger<NumericDocValuesSub> docIDMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);

                        final long finalCost = cost;
                        
                        return new NumericDocValues() {
                          private int docID = -1;
                          private NumericDocValuesSub current;

                          @Override
                          public int docID() {
                            return docID;
                          }

                          @Override
                          public int nextDoc() throws IOException {
                            current = docIDMerger.next();
                            if (current == null) {
                              docID = NO_MORE_DOCS;
                            } else {
                              docID = current.mappedDocID;
                            }
                            return docID;
                          }

                          @Override
                          public int advance(int target) throws IOException {
                            throw new UnsupportedOperationException();
                          }

                          @Override
                          public boolean advanceExact(int target) throws IOException {
                            throw new UnsupportedOperationException();
                          }

                          @Override
                          public long cost() {
                            return finalCost;
                          }

                          @Override
                          public long longValue() throws IOException {
                            return current.values.longValue();
                          }
                        };
                      }
                    });
  }
  
  /** Tracks state of one binary sub-reader that we are merging */
  private static class BinaryDocValuesSub extends DocIDMerger.Sub {

    final BinaryDocValues values;

    public BinaryDocValuesSub(MergeState.DocMap docMap, BinaryDocValues values) {
      super(docMap);
      this.values = values;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }

  /**
   * Merges the binary docvalues from <code>MergeState</code>.
   * <p>
   * The default implementation calls {@link #addBinaryField}, passing
   * a DocValuesProducer that merges and filters deleted documents on the fly.
   */
  public void mergeBinaryField(FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {
    addBinaryField(mergeFieldInfo,
                   new EmptyDocValuesProducer() {
                     @Override
                     public BinaryDocValues getBinary(FieldInfo fieldInfo) throws IOException {
                       if (fieldInfo != mergeFieldInfo) {
                         throw new IllegalArgumentException("wrong fieldInfo");
                       }
                   
                       List<BinaryDocValuesSub> subs = new ArrayList<>();

                       long cost = 0;
                       for (int i=0;i<mergeState.docValuesProducers.length;i++) {
                         BinaryDocValues values = null;
                         DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
                         if (docValuesProducer != null) {
                           FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
                           if (readerFieldInfo != null && readerFieldInfo.getDocValuesType() == DocValuesType.BINARY) {
                             values = docValuesProducer.getBinary(readerFieldInfo);
                           }
                         }
                         if (values != null) {
                           cost += values.cost();
                           subs.add(new BinaryDocValuesSub(mergeState.docMaps[i], values));
                         }
                       }

                       final DocIDMerger<BinaryDocValuesSub> docIDMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);
                       final long finalCost = cost;
                       
                       return new BinaryDocValues() {
                         private BinaryDocValuesSub current;
                         private int docID = -1;

                         @Override
                         public int docID() {
                           return docID;
                         }

                         @Override
                         public int nextDoc() throws IOException {
                           current = docIDMerger.next();
                           if (current == null) {
                             docID = NO_MORE_DOCS;
                           } else {
                             docID = current.mappedDocID;
                           }
                           return docID;
                         }

                         @Override
                         public int advance(int target) throws IOException {
                           throw new UnsupportedOperationException();
                         }

                         @Override
                         public boolean advanceExact(int target) throws IOException {
                           throw new UnsupportedOperationException();
                         }

                         @Override
                         public long cost() {
                           return finalCost;
                         }

                         @Override
                         public BytesRef binaryValue() throws IOException {
                           return current.values.binaryValue();
                         }
                       };
                     }
                   });
  }

  /** Tracks state of one sorted numeric sub-reader that we are merging */
  private static class SortedNumericDocValuesSub extends DocIDMerger.Sub {

    final SortedNumericDocValues values;

    public SortedNumericDocValuesSub(MergeState.DocMap docMap, SortedNumericDocValues values) {
      super(docMap);
      this.values = values;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }

  /**
   * Merges the sorted docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addSortedNumericField}, passing
   * iterables that filter deleted documents.
   */
  public void mergeSortedNumericField(FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {
    
    addSortedNumericField(mergeFieldInfo,
                          new EmptyDocValuesProducer() {
                            @Override
                            public SortedNumericDocValues getSortedNumeric(FieldInfo fieldInfo) throws IOException {
                              if (fieldInfo != mergeFieldInfo) {
                                throw new IllegalArgumentException("wrong FieldInfo");
                              }
                              
                              // We must make new iterators + DocIDMerger for each iterator:
                              List<SortedNumericDocValuesSub> subs = new ArrayList<>();
                              long cost = 0;
                              for (int i=0;i<mergeState.docValuesProducers.length;i++) {
                                DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
                                SortedNumericDocValues values = null;
                                if (docValuesProducer != null) {
                                  FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
                                  if (readerFieldInfo != null && readerFieldInfo.getDocValuesType() == DocValuesType.SORTED_NUMERIC) {
                                    values = docValuesProducer.getSortedNumeric(readerFieldInfo);
                                  }
                                }
                                if (values == null) {
                                  values = DocValues.emptySortedNumeric(mergeState.maxDocs[i]);
                                }
                                cost += values.cost();
                                subs.add(new SortedNumericDocValuesSub(mergeState.docMaps[i], values));
                              }

                              final long finalCost = cost;

                              final DocIDMerger<SortedNumericDocValuesSub> docIDMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);

                              return new SortedNumericDocValues() {

                                private int docID = -1;
                                private SortedNumericDocValuesSub currentSub;

                                @Override
                                public int docID() {
                                  return docID;
                                }
                                
                                @Override
                                public int nextDoc() throws IOException {
                                  currentSub = docIDMerger.next();
                                  if (currentSub == null) {
                                    docID = NO_MORE_DOCS;
                                  } else {
                                    docID = currentSub.mappedDocID;
                                  }

                                  return docID;
                                }

                                @Override
                                public int advance(int target) throws IOException {
                                  throw new UnsupportedOperationException();
                                }

                                @Override
                                public boolean advanceExact(int target) throws IOException {
                                  throw new UnsupportedOperationException();
                                }

                                @Override
                                public int docValueCount() {
                                  return currentSub.values.docValueCount();
                                }

                                @Override
                                public long cost() {
                                  return finalCost;
                                }

                                @Override
                                public long nextValue() throws IOException {
                                  return currentSub.values.nextValue();
                                }
                              };
                            }
                          });
  }

  /**
   * Merges the sorted docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addSortedNumericField}, passing
   * iterables that filter deleted documents.
   */
  public void mergeReferenceField(FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {
    
    assert mergeFieldInfo.name.substring(mergeFieldInfo.name.length() - 4).equals("$nbr");
    String vectorFieldName = mergeFieldInfo.name.substring(0, mergeFieldInfo.name.length() - 4);
    // We must compute the entire merged field in memory since each document's values depend on its neighbors
    //mergeState.infoStream.message("ReferenceDocValues", "merging " + mergeState.segmentInfo);
    List<VectorDocValuesSub> subs = new ArrayList<>();
    List<VectorDocValuesSupplier> suppliers = new ArrayList<>();
    int dimension = -1;
    for (int i = 0 ; i < mergeState.docValuesProducers.length; i++) {
      DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
      if (docValuesProducer != null) {
        FieldInfo vectorFieldInfo = mergeState.fieldInfos[i].fieldInfo(vectorFieldName);
        if (vectorFieldInfo != null && vectorFieldInfo.getDocValuesType() == DocValuesType.BINARY) {
          int segmentDimension = VectorDocValuesWriter.getDimensionFromAttribute(vectorFieldInfo);
          if (dimension == -1) {
            dimension = segmentDimension;
          } else if (dimension != segmentDimension) {
            throw new IllegalStateException("Varying dimensions for vector-valued field " + mergeFieldInfo.name
                + ": " + dimension + "!=" + segmentDimension);
          }
          VectorDocValues values = VectorDocValues.get(docValuesProducer.getBinary(vectorFieldInfo), dimension);
          suppliers.add(() -> VectorDocValues.get(docValuesProducer.getBinary(vectorFieldInfo), segmentDimension));
          subs.add(new VectorDocValuesSub(i, mergeState.docMaps[i], values));
        }
      }
    }
    // Create a new SortedNumericDocValues by iterating over the vectors, searching for
    // its nearest neighbor vectors in the newly merged segments' vectors, mapping the resulting
    // docids using docMaps in the mergeState.
    MultiVectorDV multiVectors = new MultiVectorDV(suppliers, subs, mergeState.maxDocs);
    ReferenceDocValuesWriter refWriter = new ReferenceDocValuesWriter(mergeFieldInfo, Counter.newCounter(false));
    SortedNumericDocValues refs = refWriter.getBufferedValues();
    float[] vector = new float[dimension];
    GraphSearch graphSearch = GraphSearch.fromDimension(dimension);
    int i;
    for (i = 0; i < subs.size(); i++) {
      // advance past the first document; there are no neighbors for it
      if (subs.get(i).nextDoc() != NO_MORE_DOCS) {
        break;
      }
    }
    for (; i < subs.size(); i++) {
      VectorDocValuesSub sub = subs.get(i);
      MergeState.DocMap docMap = mergeState.docMaps[sub.segmentIndex];
      // nocommit: test sorted index and test index with deletions
      int docid;
      while ((docid = sub.nextDoc()) != NO_MORE_DOCS) {
        int mappedDocId = docMap.get(docid);
        assert sub.values.docID() == docid;
        assert docid == multiVectors.unmap(mappedDocId) : "unmap mismatch " + docid + " != " + multiVectors.unmap(mappedDocId);
        sub.values.vector(vector);
        //System.out.println("merge doc " + mappedDocId + " mapped from [" + i + "," + docid + "] in thread " + Thread.currentThread().getName());
        for (ScoreDoc ref : graphSearch.search(() -> multiVectors, () -> refs, vector, mappedDocId)) {
          if (ref.doc >= 0) {
            // ignore sentinels
            //System.out.println("  ref " + ref.doc);
            refWriter.addValue(mappedDocId, ref.doc);
          }
        }
      }
    }

    addSortedNumericField(mergeFieldInfo,
        new EmptyDocValuesProducer() {
          @Override
          public SortedNumericDocValues getSortedNumeric(FieldInfo fieldInfo) {
            if (fieldInfo != mergeFieldInfo) {
              throw new IllegalArgumentException("wrong FieldInfo");
            }
            //mergeState.infoStream.message("ReferenceDocValues", "new iterator " + mergeState.segmentInfo);
            return refWriter.getIterableValues();
          }
        });

    //mergeState.infoStream.message("ReferenceDocValues", " mergeReferenceField done: " + mergeState.segmentInfo);
  }

  /** Tracks state of one binary sub-reader that we are merging */
  private static class VectorDocValuesSub extends DocIDMerger.Sub {

    final VectorDocValues values;
    final int segmentIndex;

    public VectorDocValuesSub(int segmentIndex, MergeState.DocMap docMap, VectorDocValues values) {
      super(docMap);
      this.values = values;
      this.segmentIndex = segmentIndex;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }

  // provides a view over multiple VectorDocValues by concatenating their docid spaces
  private static class MultiVectorDV extends VectorDocValues {
    private final VectorDocValues[] subValues;
    private final int[] docBase;
    private final int[] segmentMaxDocs;
    private final int cost;

    private int whichSub;

    MultiVectorDV(List<VectorDocValuesSupplier> suppliers, List<VectorDocValuesSub> subs, int[] maxDocs) throws IOException {
      this.subValues = new VectorDocValues[suppliers.size()];
      // TODO: this complicated logic needs its own test
      // maxDocs actually says *how many* docs there are, not what the number of the max doc is
      int maxDoc = -1;
      int lastMaxDoc = -1;
      segmentMaxDocs = new int[subs.size() - 1];
      docBase = new int[subs.size()];
      for (int i = 0, j = 0; j < subs.size(); i++) {
        lastMaxDoc = maxDoc;
        maxDoc += maxDocs[i];
        if (i == subs.get(j).segmentIndex) {
          // we may skip some segments if they have no docs with values for this field
          if (j > 0) {
            segmentMaxDocs[j - 1] = lastMaxDoc;
          }
          docBase[j] = lastMaxDoc + 1;
          ++j;
        }
      }

      int i = 0;
      int totalCost = 0;
      for (VectorDocValuesSupplier supplier : suppliers) {
        ResettingVectorDV sub = new ResettingVectorDV(supplier);
        totalCost += sub.cost();
        this.subValues[i++] = sub;
      }
      cost = totalCost;
      whichSub = 0;
    }

    private int findSegment(int docid) {
      int segment = Arrays.binarySearch(segmentMaxDocs, docid);
      if (segment < 0) {
        return -1 - segment;
      } else {
        return segment;
      }
    }

    @Override
    public int docID() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int dimension() {
      return subValues[0].dimension();
    }

    @Override
    public long cost() {
      return cost;
    }

    @Override
    public int nextDoc() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int advance(int target) throws IOException {
      int rebased = unmapSettingWhich(target);
      if (rebased < 0) {
        rebased = 0;
      }
      int segmentDocId = subValues[whichSub].advance(rebased);
      if (segmentDocId == NO_MORE_DOCS) {
        if (++whichSub < subValues.length) {
          // Get the first document in the next segment; Note that all segments have values.
          segmentDocId = subValues[whichSub].advance(0);
        } else {
          return NO_MORE_DOCS;
        }
      }
      return docBase[whichSub] + segmentDocId;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      int rebased = unmapSettingWhich(target);
      if (rebased < 0) {
        return false;
      }
      return subValues[whichSub].advanceExact(rebased);
    }

    int unmap(int docid) {
      // map from global (merged) to segment-local (unmerged)
      // like mapDocid but no side effect - used for assertion
      return docid - docBase[findSegment(docid)];
    }

    private int unmapSettingWhich(int target) {
      whichSub = findSegment(target);
      return target - docBase[whichSub];
    }

    @Override
    public void vector(float[] vector) throws IOException {
      subValues[whichSub].vector(vector);
    }
  }

  // provides pseudo-random access to the values as float[] by recreating an underlying
  // iterator whenever the iteration goes backwards
  private static class ResettingVectorDV extends VectorDocValues {

    private final VectorDocValuesSupplier supplier;
    private VectorDocValues delegate;
    private int docId = -1;

    ResettingVectorDV(VectorDocValuesSupplier supplier) throws IOException {
      this.supplier = supplier;
      delegate = supplier.get();
    }

    @Override
    public int docID() {
      if (docId < 0) {
        return -docId;
      } else {
        return docId;
      }
    }

    @Override
    public int dimension() {
      return delegate.dimension();
    }

    @Override
    public long cost() {
      return delegate.cost();
    }

    @Override
    public int nextDoc() throws IOException {
      docId = delegate.nextDoc();
      return docId;
    }

    @Override
    public int advance(int target) throws IOException {
      if (target == docId) {
        return target;
      }
      maybeReset(target);
      docId = delegate.advance(target);
      return docId;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      if (target == docId) {
        return true;
      }
      maybeReset(target);
      boolean advanced = delegate.advanceExact(target);
      if (advanced) {
        docId = delegate.docID();
      } else {
        docId = -delegate.docID();
      }
      return advanced;
    }

    @Override
    public void vector(float[] vector) throws IOException {
      delegate.vector(vector);
    }

    private void maybeReset(int target) throws IOException {
      if (target < delegate.docID()) {
        delegate = supplier.get();
      }
    }
  }

  private interface VectorDocValuesSupplier {
    VectorDocValues get() throws IOException;
  }

  /** Tracks state of one sorted sub-reader that we are merging */
  private static class SortedDocValuesSub extends DocIDMerger.Sub {

    final SortedDocValues values;
    final LongValues map;
    
    public SortedDocValuesSub(MergeState.DocMap docMap, SortedDocValues values, LongValues map) {
      super(docMap);
      this.values = values;
      this.map = map;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }

  /**
   * Merges the sorted docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addSortedField}, passing
   * an Iterable that merges ordinals and values and filters deleted documents .
   */
  public void mergeSortedField(FieldInfo fieldInfo, final MergeState mergeState) throws IOException {
    List<SortedDocValues> toMerge = new ArrayList<>();
    for (int i=0;i<mergeState.docValuesProducers.length;i++) {
      SortedDocValues values = null;
      DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
      if (docValuesProducer != null) {
        FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(fieldInfo.name);
        if (readerFieldInfo != null && readerFieldInfo.getDocValuesType() == DocValuesType.SORTED) {
          values = docValuesProducer.getSorted(fieldInfo);
        }
      }
      if (values == null) {
        values = DocValues.emptySorted();
      }
      toMerge.add(values);
    }

    final int numReaders = toMerge.size();
    final SortedDocValues dvs[] = toMerge.toArray(new SortedDocValues[numReaders]);
    
    // step 1: iterate thru each sub and mark terms still in use
    TermsEnum liveTerms[] = new TermsEnum[dvs.length];
    long[] weights = new long[liveTerms.length];
    for (int sub=0;sub<numReaders;sub++) {
      SortedDocValues dv = dvs[sub];
      Bits liveDocs = mergeState.liveDocs[sub];
      if (liveDocs == null) {
        liveTerms[sub] = dv.termsEnum();
        weights[sub] = dv.getValueCount();
      } else {
        LongBitSet bitset = new LongBitSet(dv.getValueCount());
        int docID;
        while ((docID = dv.nextDoc()) != NO_MORE_DOCS) {
          if (liveDocs.get(docID)) {
            int ord = dv.ordValue();
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
    final OrdinalMap map = OrdinalMap.build(null, liveTerms, weights, PackedInts.COMPACT);
    
    // step 3: add field
    addSortedField(fieldInfo,
                   new EmptyDocValuesProducer() {
                     @Override
                     public SortedDocValues getSorted(FieldInfo fieldInfoIn) throws IOException {
                       if (fieldInfoIn != fieldInfo) {
                         throw new IllegalArgumentException("wrong FieldInfo");
                       }

                       // We must make new iterators + DocIDMerger for each iterator:

                       List<SortedDocValuesSub> subs = new ArrayList<>();
                       long cost = 0;
                       for (int i=0;i<mergeState.docValuesProducers.length;i++) {
                         SortedDocValues values = null;
                         DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
                         if (docValuesProducer != null) {
                           FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(fieldInfo.name);
                           if (readerFieldInfo != null && readerFieldInfo.getDocValuesType() == DocValuesType.SORTED) {
                             values = docValuesProducer.getSorted(readerFieldInfo);
                           }
                         }
                         if (values == null) {
                           values = DocValues.emptySorted();
                         }
                         cost += values.cost();
                         
                         subs.add(new SortedDocValuesSub(mergeState.docMaps[i], values, map.getGlobalOrds(i)));
                       }

                       final long finalCost = cost;

                       final DocIDMerger<SortedDocValuesSub> docIDMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);
                       
                       return new SortedDocValues() {
                         private int docID = -1;
                         private int ord;

                         @Override
                         public int docID() {
                           return docID;
                         }

                         @Override
                         public int nextDoc() throws IOException {
                           SortedDocValuesSub sub = docIDMerger.next();
                           if (sub == null) {
                             return docID = NO_MORE_DOCS;
                           }
                           int subOrd = sub.values.ordValue();
                           assert subOrd != -1;
                           ord = (int) sub.map.get(subOrd);
                           docID = sub.mappedDocID;
                           return docID;
                         }

                         @Override
                         public int ordValue() {
                           return ord;
                         }
                         
                         @Override
                         public int advance(int target) {
                           throw new UnsupportedOperationException();
                         }

                         @Override
                         public boolean advanceExact(int target) throws IOException {
                           throw new UnsupportedOperationException();
                         }

                         @Override
                         public long cost() {
                           return finalCost;
                         }

                         @Override
                         public int getValueCount() {
                           return (int) map.getValueCount();
                         }
                         
                         @Override
                         public BytesRef lookupOrd(int ord) throws IOException {
                           int segmentNumber = map.getFirstSegmentNumber(ord);
                           int segmentOrd = (int) map.getFirstSegmentOrd(ord);
                           return dvs[segmentNumber].lookupOrd(segmentOrd);
                         }
                       };
                     }
                   });
  }
  
  /** Tracks state of one sorted set sub-reader that we are merging */
  private static class SortedSetDocValuesSub extends DocIDMerger.Sub {

    final SortedSetDocValues values;
    final LongValues map;
    
    public SortedSetDocValuesSub(MergeState.DocMap docMap, SortedSetDocValues values, LongValues map) {
      super(docMap);
      this.values = values;
      this.map = map;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }

    @Override
    public String toString() {
      return "SortedSetDocValuesSub(mappedDocID=" + mappedDocID + " values=" + values + ")";
    }
  }

  /**
   * Merges the sortedset docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addSortedSetField}, passing
   * an Iterable that merges ordinals and values and filters deleted documents .
   */
  public void mergeSortedSetField(FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {

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

    // step 1: iterate thru each sub and mark terms still in use
    TermsEnum liveTerms[] = new TermsEnum[toMerge.size()];
    long[] weights = new long[liveTerms.length];
    for (int sub = 0; sub < liveTerms.length; sub++) {
      SortedSetDocValues dv = toMerge.get(sub);
      Bits liveDocs = mergeState.liveDocs[sub];
      if (liveDocs == null) {
        liveTerms[sub] = dv.termsEnum();
        weights[sub] = dv.getValueCount();
      } else {
        LongBitSet bitset = new LongBitSet(dv.getValueCount());
        int docID;
        while ((docID = dv.nextDoc()) != NO_MORE_DOCS) {
          if (liveDocs.get(docID)) {
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
    final OrdinalMap map = OrdinalMap.build(null, liveTerms, weights, PackedInts.COMPACT);
    
    // step 3: add field
    addSortedSetField(mergeFieldInfo,
                      new EmptyDocValuesProducer() {
                        @Override
                        public SortedSetDocValues getSortedSet(FieldInfo fieldInfo) throws IOException {
                          if (fieldInfo != mergeFieldInfo) {
                            throw new IllegalArgumentException("wrong FieldInfo");
                          }

                          // We must make new iterators + DocIDMerger for each iterator:
                          List<SortedSetDocValuesSub> subs = new ArrayList<>();

                          long cost = 0;
                          
                          for (int i=0;i<mergeState.docValuesProducers.length;i++) {
                            SortedSetDocValues values = null;
                            DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
                            if (docValuesProducer != null) {
                              FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
                              if (readerFieldInfo != null && readerFieldInfo.getDocValuesType() == DocValuesType.SORTED_SET) {
                                values = docValuesProducer.getSortedSet(readerFieldInfo);
                              }
                            }
                            if (values == null) {
                              values = DocValues.emptySortedSet();
                            }
                            cost += values.cost();
                            subs.add(new SortedSetDocValuesSub(mergeState.docMaps[i], values, map.getGlobalOrds(i)));
                          }
            
                          final DocIDMerger<SortedSetDocValuesSub> docIDMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);
                          
                          final long finalCost = cost;

                          return new SortedSetDocValues() {
                            private int docID = -1;
                            private SortedSetDocValuesSub currentSub;

                            @Override
                            public int docID() {
                              return docID;
                            }

                            @Override
                            public int nextDoc() throws IOException {
                              currentSub = docIDMerger.next();
                              if (currentSub == null) {
                                docID = NO_MORE_DOCS;
                              } else {
                                docID = currentSub.mappedDocID;
                              }

                              return docID;
                            }

                            @Override
                            public int advance(int target) throws IOException {
                              throw new UnsupportedOperationException();
                            }

                            @Override
                            public boolean advanceExact(int target) throws IOException {
                              throw new UnsupportedOperationException();
                            }

                            @Override
                            public long nextOrd() throws IOException {
                              long subOrd = currentSub.values.nextOrd();
                              if (subOrd == NO_MORE_ORDS) {
                                return NO_MORE_ORDS;
                              }
                              return currentSub.map.get(subOrd);
                            }

                            @Override
                            public long cost() {
                              return finalCost;
                            }

                            @Override
                            public BytesRef lookupOrd(long ord) throws IOException {
                              int segmentNumber = map.getFirstSegmentNumber(ord);
                              long segmentOrd = map.getFirstSegmentOrd(ord);
                              return toMerge.get(segmentNumber).lookupOrd(segmentOrd);
                            }

                            @Override
                            public long getValueCount() {
                              return map.getValueCount();
                            }
                          };
                        }
                      });
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
