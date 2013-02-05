package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.MultiTermsEnum.TermsEnumIndex;
import org.apache.lucene.index.MultiTermsEnum.TermsEnumWithSlice;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.AppendingLongBuffer;

/**
 * A wrapper for CompositeIndexReader providing access to DocValues.
 * 
 * <p><b>NOTE</b>: for multi readers, you'll get better
 * performance by gathering the sub readers using
 * {@link IndexReader#getContext()} to get the
 * atomic leaves and then operate per-AtomicReader,
 * instead of using this class.
 * 
 * <p><b>NOTE</b>: This is very costly.
 *
 * @lucene.experimental
 * @lucene.internal
 */
public class MultiDocValues {
  
  /** No instantiation */
  private MultiDocValues() {}
  
  /** Returns a NumericDocValues for a reader's norms (potentially merging on-the-fly).
   * <p>
   * This is a slow way to access normalization values. Instead, access them per-segment
   * with {@link AtomicReader#getNormValues(String)}
   * </p> 
   */
  public static NumericDocValues getNormValues(final IndexReader r, final String field) throws IOException {
    final List<AtomicReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getNormValues(field);
    }
    FieldInfo fi = MultiFields.getMergedFieldInfos(r).fieldInfo(field);
    if (fi == null || fi.hasNorms() == false) {
      return null;
    }

    boolean anyReal = false;
    final NumericDocValues[] values = new NumericDocValues[size];
    final int[] starts = new int[size+1];
    for (int i = 0; i < size; i++) {
      AtomicReaderContext context = leaves.get(i);
      NumericDocValues v = context.reader().getNormValues(field);
      if (v == null) {
        v = NumericDocValues.EMPTY;
      } else {
        anyReal = true;
      }
      values[i] = v;
      starts[i] = context.docBase;
    }
    starts[size] = r.maxDoc();
    
    assert anyReal;

    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        int subIndex = ReaderUtil.subIndex(docID, starts);
        return values[subIndex].get(docID - starts[subIndex]);
      }
    };
  }

  /** Returns a NumericDocValues for a reader's docvalues (potentially merging on-the-fly) 
   * <p>
   * This is a slow way to access numeric values. Instead, access them per-segment
   * with {@link AtomicReader#getNumericDocValues(String)}
   * </p> 
   * */
  public static NumericDocValues getNumericValues(final IndexReader r, final String field) throws IOException {
    final List<AtomicReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getNumericDocValues(field);
    }

    boolean anyReal = false;
    final NumericDocValues[] values = new NumericDocValues[size];
    final int[] starts = new int[size+1];
    for (int i = 0; i < size; i++) {
      AtomicReaderContext context = leaves.get(i);
      NumericDocValues v = context.reader().getNumericDocValues(field);
      if (v == null) {
        v = NumericDocValues.EMPTY;
      } else {
        anyReal = true;
      }
      values[i] = v;
      starts[i] = context.docBase;
    }
    starts[size] = r.maxDoc();

    if (!anyReal) {
      return null;
    } else {
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          int subIndex = ReaderUtil.subIndex(docID, starts);
          return values[subIndex].get(docID - starts[subIndex]);
        }
      };
    }
  }

  /** Returns a BinaryDocValues for a reader's docvalues (potentially merging on-the-fly)
   * <p>
   * This is a slow way to access binary values. Instead, access them per-segment
   * with {@link AtomicReader#getBinaryDocValues(String)}
   * </p>  
   */
  public static BinaryDocValues getBinaryValues(final IndexReader r, final String field) throws IOException {
    final List<AtomicReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getBinaryDocValues(field);
    }
    
    boolean anyReal = false;
    final BinaryDocValues[] values = new BinaryDocValues[size];
    final int[] starts = new int[size+1];
    for (int i = 0; i < size; i++) {
      AtomicReaderContext context = leaves.get(i);
      BinaryDocValues v = context.reader().getBinaryDocValues(field);
      if (v == null) {
        v = BinaryDocValues.EMPTY;
      } else {
        anyReal = true;
      }
      values[i] = v;
      starts[i] = context.docBase;
    }
    starts[size] = r.maxDoc();
    
    if (!anyReal) {
      return null;
    } else {
      return new BinaryDocValues() {
        @Override
        public void get(int docID, BytesRef result) {
          int subIndex = ReaderUtil.subIndex(docID, starts);
          values[subIndex].get(docID - starts[subIndex], result);
        }
      };
    }
  }
  
  /** Returns a SortedDocValues for a reader's docvalues (potentially doing extremely slow things).
   * <p>
   * This is an extremely slow way to access sorted values. Instead, access them per-segment
   * with {@link AtomicReader#getSortedDocValues(String)}
   * </p>  
   */
  public static SortedDocValues getSortedValues(final IndexReader r, final String field) throws IOException {
    final List<AtomicReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getSortedDocValues(field);
    }
    
    boolean anyReal = false;
    final SortedDocValues[] values = new SortedDocValues[size];
    final int[] starts = new int[size+1];
    for (int i = 0; i < size; i++) {
      AtomicReaderContext context = leaves.get(i);
      SortedDocValues v = context.reader().getSortedDocValues(field);
      if (v == null) {
        v = SortedDocValues.EMPTY;
      } else {
        anyReal = true;
      }
      values[i] = v;
      starts[i] = context.docBase;
    }
    starts[size] = r.maxDoc();
    
    if (!anyReal) {
      return null;
    } else {
      OrdinalMap mapping = new OrdinalMap(values);
      return new MultiSortedDocValues(values, starts, mapping);
    }
  }
  
  /** maps per-segment ordinals to/from global ordinal space */
  // TODO: use more efficient packed ints structures (these are all positive values!)
  static class OrdinalMap {
    // globalOrd -> (globalOrd - segmentOrd)
    final AppendingLongBuffer globalOrdDeltas;
    // globalOrd -> sub index
    final AppendingLongBuffer subIndexes;
    // segmentOrd -> (globalOrd - segmentOrd)
    final AppendingLongBuffer ordDeltas[];
    
    OrdinalMap(SortedDocValues subs[]) throws IOException {
      // create the ordinal mappings by pulling a termsenum over each sub's 
      // unique terms, and walking a multitermsenum over those
      globalOrdDeltas = new AppendingLongBuffer();
      subIndexes = new AppendingLongBuffer();
      ordDeltas = new AppendingLongBuffer[subs.length];
      for (int i = 0; i < ordDeltas.length; i++) {
        ordDeltas[i] = new AppendingLongBuffer();
      }
      int segmentOrds[] = new int[subs.length];
      ReaderSlice slices[] = new ReaderSlice[subs.length];
      TermsEnumIndex indexes[] = new TermsEnumIndex[slices.length];
      for (int i = 0; i < slices.length; i++) {
        slices[i] = new ReaderSlice(0, 0, i);
        indexes[i] = new TermsEnumIndex(new SortedDocValuesTermsEnum(subs[i]), i);
      }
      MultiTermsEnum mte = new MultiTermsEnum(slices);
      mte.reset(indexes);
      int globalOrd = 0;
      while (mte.next() != null) {        
        TermsEnumWithSlice matches[] = mte.getMatchArray();
        for (int i = 0; i < mte.getMatchCount(); i++) {
          int subIndex = matches[i].index;
          int delta = globalOrd - segmentOrds[subIndex];
          assert delta >= 0;
          // for each unique term, just mark the first subindex/delta where it occurs
          if (i == 0) {
            subIndexes.add(subIndex);
            globalOrdDeltas.add(delta);
          }
          // for each per-segment ord, map it back to the global term.
          ordDeltas[subIndex].add(delta);
          segmentOrds[subIndex]++;
        }
        globalOrd++;
      }
    }
  }
  
  /** implements SortedDocValues over n subs, using an OrdinalMap */
  static class MultiSortedDocValues extends SortedDocValues {
    final int docStarts[];
    final SortedDocValues values[];
    final OrdinalMap mapping;
  
    MultiSortedDocValues(SortedDocValues values[], int docStarts[], OrdinalMap mapping) throws IOException {
      this.values = values;
      this.docStarts = docStarts;
      this.mapping = mapping;
    }
       
    @Override
    public int getOrd(int docID) {
      int subIndex = ReaderUtil.subIndex(docID, docStarts);
      int segmentOrd = values[subIndex].getOrd(docID - docStarts[subIndex]);
      return (int) (segmentOrd + mapping.ordDeltas[subIndex].get(segmentOrd));
    }
 
    @Override
    public void lookupOrd(int ord, BytesRef result) {
      int subIndex = (int) mapping.subIndexes.get(ord);
      int segmentOrd = (int) (ord - mapping.globalOrdDeltas.get(ord));
      assert subIndex < values.length;
      values[subIndex].lookupOrd(segmentOrd, result);
    }
 
    @Override
    public int getValueCount() {
      return mapping.globalOrdDeltas.size();
    }
  }
}
