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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.PackedInts;

/**
 * A wrapper for CompositeIndexReader providing access to DocValues.
 * 
 * <p><b>NOTE</b>: for multi readers, you'll get better
 * performance by gathering the sub readers using
 * {@link IndexReader#getContext()} to get the
 * atomic leaves and then operate per-LeafReader,
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
   * with {@link LeafReader#getNormValues(String)}
   * </p> 
   */
  public static NumericDocValues getNormValues(final IndexReader r, final String field) throws IOException {
    final List<LeafReaderContext> leaves = r.leaves();
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

    return new NumericDocValues() {
      private int nextLeaf;
      private NumericDocValues currentValues;
      private LeafReaderContext currentLeaf;
      private int docID = -1;

      @Override
      public int nextDoc() throws IOException {
        while (true) {
          if (currentValues == null) {
            if (nextLeaf == leaves.size()) {
              docID = NO_MORE_DOCS;
              return docID;
            }
            currentLeaf = leaves.get(nextLeaf);
            currentValues = currentLeaf.reader().getNormValues(field);
            nextLeaf++;
            continue;
          }

          int newDocID = currentValues.nextDoc();

          if (newDocID == NO_MORE_DOCS) {
            currentValues = null;
            continue;
          } else {
            docID = currentLeaf.docBase + newDocID;
            return docID;
          }
        }
      }
        
      @Override
      public int docID() {
        return docID;
      }

      @Override
      public int advance(int targetDocID) throws IOException {
        if (targetDocID <= docID) {
          throw new IllegalArgumentException("can only advance beyond current document: on docID=" + docID + " but targetDocID=" + targetDocID);
        }
        int readerIndex = ReaderUtil.subIndex(targetDocID, leaves);
        if (readerIndex >= nextLeaf) {
          if (readerIndex == leaves.size()) {
            currentValues = null;
            docID = NO_MORE_DOCS;
            return docID;
          }
          currentLeaf = leaves.get(readerIndex);
          currentValues = currentLeaf.reader().getNormValues(field);
          if (currentValues == null) {
            return nextDoc();
          }
          nextLeaf = readerIndex+1;
        }
        int newDocID = currentValues.advance(targetDocID - currentLeaf.docBase);
        if (newDocID == NO_MORE_DOCS) {
          currentValues = null;
          return nextDoc();
        } else {
          docID = currentLeaf.docBase + newDocID;
          return docID;
        }
      }

      @Override
      public boolean advanceExact(int targetDocID) throws IOException {
        if (targetDocID < docID) {
          throw new IllegalArgumentException("can only advance beyond current document: on docID=" + docID + " but targetDocID=" + targetDocID);
        }
        int readerIndex = ReaderUtil.subIndex(targetDocID, leaves);
        if (readerIndex >= nextLeaf) {
          if (readerIndex == leaves.size()) {
            throw new IllegalArgumentException("Out of range: " + targetDocID);
          }
          currentLeaf = leaves.get(readerIndex);
          currentValues = currentLeaf.reader().getNormValues(field);
          nextLeaf = readerIndex+1;
        }
        docID = targetDocID;
        if (currentValues == null) {
          return false;
        }
        return currentValues.advanceExact(targetDocID - currentLeaf.docBase);
      }

      @Override
      public long longValue() throws IOException {
        return currentValues.longValue();
      }

      @Override
      public long cost() {
        // TODO
        return 0;
      }
    };
  }

  /** Returns a NumericDocValues for a reader's docvalues (potentially merging on-the-fly) */
  public static NumericDocValues getNumericValues(final IndexReader r, final String field) throws IOException {
    final List<LeafReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getNumericDocValues(field);
    }

    boolean anyReal = false;
    for(LeafReaderContext leaf : leaves) {
      FieldInfo fieldInfo = leaf.reader().getFieldInfos().fieldInfo(field);
      if (fieldInfo != null) {
        DocValuesType dvType = fieldInfo.getDocValuesType();
        if (dvType == DocValuesType.NUMERIC) {
          anyReal = true;
          break;
        }
      }
    }

    if (anyReal == false) {
      return null;
    }
    

    return new NumericDocValues() {
      private int nextLeaf;
      private NumericDocValues currentValues;
      private LeafReaderContext currentLeaf;
      private int docID = -1;

      @Override
      public int docID() {
        return docID;
      }

      @Override
      public int nextDoc() throws IOException {
        while (true) {
          while (currentValues == null) {
            if (nextLeaf == leaves.size()) {
              docID = NO_MORE_DOCS;
              return docID;
            }
            currentLeaf = leaves.get(nextLeaf);
            currentValues = currentLeaf.reader().getNumericDocValues(field);
            nextLeaf++;
          }

          int newDocID = currentValues.nextDoc();

          if (newDocID == NO_MORE_DOCS) {
            currentValues = null;
            continue;
          } else {
            docID = currentLeaf.docBase + newDocID;
            return docID;
          }
        }
      }
        
      @Override
      public int advance(int targetDocID) throws IOException {
        if (targetDocID <= docID) {
          throw new IllegalArgumentException("can only advance beyond current document: on docID=" + docID + " but targetDocID=" + targetDocID);
        }
        int readerIndex = ReaderUtil.subIndex(targetDocID, leaves);
        if (readerIndex >= nextLeaf) {
          if (readerIndex == leaves.size()) {
            currentValues = null;
            docID = NO_MORE_DOCS;
            return docID;
          }
          currentLeaf = leaves.get(readerIndex);
          currentValues = currentLeaf.reader().getNumericDocValues(field);
          nextLeaf = readerIndex+1;
          if (currentValues == null) {
            return nextDoc();
          }
        }
        int newDocID = currentValues.advance(targetDocID - currentLeaf.docBase);
        if (newDocID == NO_MORE_DOCS) {
          currentValues = null;
          return nextDoc();
        } else {
          docID = currentLeaf.docBase + newDocID;
          return docID;
        }
      }

      @Override
      public boolean advanceExact(int targetDocID) throws IOException {
        if (targetDocID < docID) {
          throw new IllegalArgumentException("can only advance beyond current document: on docID=" + docID + " but targetDocID=" + targetDocID);
        }
        int readerIndex = ReaderUtil.subIndex(targetDocID, leaves);
        if (readerIndex >= nextLeaf) {
          if (readerIndex == leaves.size()) {
            throw new IllegalArgumentException("Out of range: " + targetDocID);
          }
          currentLeaf = leaves.get(readerIndex);
          currentValues = currentLeaf.reader().getNumericDocValues(field);
          nextLeaf = readerIndex+1;
        }
        docID = targetDocID;
        if (currentValues == null) {
          return false;
        }
        return currentValues.advanceExact(targetDocID - currentLeaf.docBase);
      }
      @Override
      public long longValue() throws IOException {
        return currentValues.longValue();
      }

      @Override
      public long cost() {
        // TODO
        return 0;
      }
    };
  }

  /** Returns a BinaryDocValues for a reader's docvalues (potentially merging on-the-fly) */
  public static BinaryDocValues getBinaryValues(final IndexReader r, final String field) throws IOException {
    final List<LeafReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getBinaryDocValues(field);
    }

    boolean anyReal = false;
    for(LeafReaderContext leaf : leaves) {
      FieldInfo fieldInfo = leaf.reader().getFieldInfos().fieldInfo(field);
      if (fieldInfo != null) {
        DocValuesType dvType = fieldInfo.getDocValuesType();
        if (dvType == DocValuesType.BINARY) {
          anyReal = true;
          break;
        }
      }
    }

    if (anyReal == false) {
      return null;
    }

    return new BinaryDocValues() {
      private int nextLeaf;
      private BinaryDocValues currentValues;
      private LeafReaderContext currentLeaf;
      private int docID = -1;

      @Override
      public int nextDoc() throws IOException {
        while (true) {
          while (currentValues == null) {
            if (nextLeaf == leaves.size()) {
              docID = NO_MORE_DOCS;
              return docID;
            }
            currentLeaf = leaves.get(nextLeaf);
            currentValues = currentLeaf.reader().getBinaryDocValues(field);
            nextLeaf++;
          }

          int newDocID = currentValues.nextDoc();

          if (newDocID == NO_MORE_DOCS) {
            currentValues = null;
            continue;
          } else {
            docID = currentLeaf.docBase + newDocID;
            return docID;
          }
        }
      }
        
      @Override
      public int docID() {
        return docID;
      }

      @Override
      public int advance(int targetDocID) throws IOException {
        if (targetDocID <= docID) {
          throw new IllegalArgumentException("can only advance beyond current document: on docID=" + docID + " but targetDocID=" + targetDocID);
        }
        int readerIndex = ReaderUtil.subIndex(targetDocID, leaves);
        if (readerIndex >= nextLeaf) {
          if (readerIndex == leaves.size()) {
            currentValues = null;
            docID = NO_MORE_DOCS;
            return docID;
          }
          currentLeaf = leaves.get(readerIndex);
          currentValues = currentLeaf.reader().getBinaryDocValues(field);
          nextLeaf = readerIndex+1;
          if (currentValues == null) {
            return nextDoc();
          }
        }
        int newDocID = currentValues.advance(targetDocID - currentLeaf.docBase);
        if (newDocID == NO_MORE_DOCS) {
          currentValues = null;
          return nextDoc();
        } else {
          docID = currentLeaf.docBase + newDocID;
          return docID;
        }
      }

      @Override
      public boolean advanceExact(int targetDocID) throws IOException {
        if (targetDocID < docID) {
          throw new IllegalArgumentException("can only advance beyond current document: on docID=" + docID + " but targetDocID=" + targetDocID);
        }
        int readerIndex = ReaderUtil.subIndex(targetDocID, leaves);
        if (readerIndex >= nextLeaf) {
          if (readerIndex == leaves.size()) {
            throw new IllegalArgumentException("Out of range: " + targetDocID);
          }
          currentLeaf = leaves.get(readerIndex);
          currentValues = currentLeaf.reader().getBinaryDocValues(field);
          nextLeaf = readerIndex+1;
        }
        docID = targetDocID;
        if (currentValues == null) {
          return false;
        }
        return currentValues.advanceExact(targetDocID - currentLeaf.docBase);
      }

      @Override
      public BytesRef binaryValue() throws IOException {
        return currentValues.binaryValue();
      }

      @Override
      public long cost() {
        // TODO
        return 0;
      }
    };
  }

  /** Returns a SortedNumericDocValues for a reader's docvalues (potentially merging on-the-fly) 
   * <p>
   * This is a slow way to access sorted numeric values. Instead, access them per-segment
   * with {@link LeafReader#getSortedNumericDocValues(String)}
   * </p> 
   * */
  public static SortedNumericDocValues getSortedNumericValues(final IndexReader r, final String field) throws IOException {
    final List<LeafReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getSortedNumericDocValues(field);
    }

    boolean anyReal = false;
    final SortedNumericDocValues[] values = new SortedNumericDocValues[size];
    final int[] starts = new int[size+1];
    long totalCost = 0;
    for (int i = 0; i < size; i++) {
      LeafReaderContext context = leaves.get(i);
      SortedNumericDocValues v = context.reader().getSortedNumericDocValues(field);
      if (v == null) {
        v = DocValues.emptySortedNumeric(context.reader().maxDoc());
      } else {
        anyReal = true;
      }
      values[i] = v;
      starts[i] = context.docBase;
      totalCost += v.cost();
    }
    starts[size] = r.maxDoc();

    if (anyReal == false) {
      return null;
    }

    final long finalTotalCost = totalCost;
    
    return new SortedNumericDocValues() {
      private int nextLeaf;
      private SortedNumericDocValues currentValues;
      private LeafReaderContext currentLeaf;
      private int docID = -1;

      @Override
      public int nextDoc() throws IOException {
        while (true) {
          if (currentValues == null) {
            if (nextLeaf == leaves.size()) {
              docID = NO_MORE_DOCS;
              return docID;
            }
            currentLeaf = leaves.get(nextLeaf);
            currentValues = values[nextLeaf];
            nextLeaf++;
          }

          int newDocID = currentValues.nextDoc();

          if (newDocID == NO_MORE_DOCS) {
            currentValues = null;
            continue;
          } else {
            docID = currentLeaf.docBase + newDocID;
            return docID;
          }
        }
      }
        
      @Override
      public int docID() {
        return docID;
      }
        
      @Override
      public int advance(int targetDocID) throws IOException {
        if (targetDocID <= docID) {
          throw new IllegalArgumentException("can only advance beyond current document: on docID=" + docID + " but targetDocID=" + targetDocID);
        }
        int readerIndex = ReaderUtil.subIndex(targetDocID, leaves);
        if (readerIndex >= nextLeaf) {
          if (readerIndex == leaves.size()) {
            currentValues = null;
            docID = NO_MORE_DOCS;
            return docID;
          }
          currentLeaf = leaves.get(readerIndex);
          currentValues = values[readerIndex];
          nextLeaf = readerIndex+1;
        }
        int newDocID = currentValues.advance(targetDocID - currentLeaf.docBase);
        if (newDocID == NO_MORE_DOCS) {
          currentValues = null;
          return nextDoc();
        } else {
          docID = currentLeaf.docBase + newDocID;
          return docID;
        }
      }

      @Override
      public boolean advanceExact(int targetDocID) throws IOException {
        if (targetDocID < docID) {
          throw new IllegalArgumentException("can only advance beyond current document: on docID=" + docID + " but targetDocID=" + targetDocID);
        }
        int readerIndex = ReaderUtil.subIndex(targetDocID, leaves);
        if (readerIndex >= nextLeaf) {
          if (readerIndex == leaves.size()) {
            throw new IllegalArgumentException("Out of range: " + targetDocID);
          }
          currentLeaf = leaves.get(readerIndex);
          currentValues = values[readerIndex];
          nextLeaf = readerIndex+1;
        }
        docID = targetDocID;
        if (currentValues == null) {
          return false;
        }
        return currentValues.advanceExact(targetDocID - currentLeaf.docBase);
      }

      @Override
      public long cost() {
        return finalTotalCost;
      }
      
      @Override
      public int docValueCount() {
        return currentValues.docValueCount();
      }

      @Override
      public long nextValue() throws IOException {
        return currentValues.nextValue();
      }
    };
  }
  
  /** Returns a SortedDocValues for a reader's docvalues (potentially doing extremely slow things).
   * <p>
   * This is an extremely slow way to access sorted values. Instead, access them per-segment
   * with {@link LeafReader#getSortedDocValues(String)}
   * </p>  
   */
  public static SortedDocValues getSortedValues(final IndexReader r, final String field) throws IOException {
    final List<LeafReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getSortedDocValues(field);
    }
    
    boolean anyReal = false;
    final SortedDocValues[] values = new SortedDocValues[size];
    final int[] starts = new int[size+1];
    long totalCost = 0;
    for (int i = 0; i < size; i++) {
      LeafReaderContext context = leaves.get(i);
      SortedDocValues v = context.reader().getSortedDocValues(field);
      if (v == null) {
        v = DocValues.emptySorted();
      } else {
        anyReal = true;
        totalCost += v.cost();
      }
      values[i] = v;
      starts[i] = context.docBase;
    }
    starts[size] = r.maxDoc();
    
    if (anyReal == false) {
      return null;
    } else {
      IndexReader.CacheHelper cacheHelper = r.getReaderCacheHelper();
      IndexReader.CacheKey owner = cacheHelper == null ? null : cacheHelper.getKey();
      OrdinalMap mapping = OrdinalMap.build(owner, values, PackedInts.DEFAULT);
      return new MultiSortedDocValues(values, starts, mapping, totalCost);
    }
  }
  
  /** Returns a SortedSetDocValues for a reader's docvalues (potentially doing extremely slow things).
   * <p>
   * This is an extremely slow way to access sorted values. Instead, access them per-segment
   * with {@link LeafReader#getSortedSetDocValues(String)}
   * </p>  
   */
  public static SortedSetDocValues getSortedSetValues(final IndexReader r, final String field) throws IOException {
    final List<LeafReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getSortedSetDocValues(field);
    }
    
    boolean anyReal = false;
    final SortedSetDocValues[] values = new SortedSetDocValues[size];
    final int[] starts = new int[size+1];
    long totalCost = 0;
    for (int i = 0; i < size; i++) {
      LeafReaderContext context = leaves.get(i);
      SortedSetDocValues v = context.reader().getSortedSetDocValues(field);
      if (v == null) {
        v = DocValues.emptySortedSet();
      } else {
        anyReal = true;
        totalCost += v.cost();
      }
      values[i] = v;
      starts[i] = context.docBase;
    }
    starts[size] = r.maxDoc();
    
    if (anyReal == false) {
      return null;
    } else {
      IndexReader.CacheHelper cacheHelper = r.getReaderCacheHelper();
      IndexReader.CacheKey owner = cacheHelper == null ? null : cacheHelper.getKey();
      OrdinalMap mapping = OrdinalMap.build(owner, values, PackedInts.DEFAULT);
      return new MultiSortedSetDocValues(values, starts, mapping, totalCost);
    }
  }

  /** 
   * Implements SortedDocValues over n subs, using an OrdinalMap
   * @lucene.internal
   */
  public static class MultiSortedDocValues extends SortedDocValues {
    /** docbase for each leaf: parallel with {@link #values} */
    public final int docStarts[];
    /** leaf values */
    public final SortedDocValues values[];
    /** ordinal map mapping ords from <code>values</code> to global ord space */
    public final OrdinalMap mapping;
    private final long totalCost;

    private int nextLeaf;
    private SortedDocValues currentValues;
    private int currentDocStart;
    private int docID = -1;    
  
    /** Creates a new MultiSortedDocValues over <code>values</code> */
    public MultiSortedDocValues(SortedDocValues values[], int docStarts[], OrdinalMap mapping, long totalCost) throws IOException {
      assert docStarts.length == values.length + 1;
      this.values = values;
      this.docStarts = docStarts;
      this.mapping = mapping;
      this.totalCost = totalCost;
    }
       
    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextDoc() throws IOException {
      while (true) {
        while (currentValues == null) {
          if (nextLeaf == values.length) {
            docID = NO_MORE_DOCS;
            return docID;
          }
          currentDocStart = docStarts[nextLeaf];
          currentValues = values[nextLeaf];
          nextLeaf++;
        }

        int newDocID = currentValues.nextDoc();

        if (newDocID == NO_MORE_DOCS) {
          currentValues = null;
          continue;
        } else {
          docID = currentDocStart + newDocID;
          return docID;
        }
      }
    }

    @Override
    public int advance(int targetDocID) throws IOException {
      if (targetDocID <= docID) {
        throw new IllegalArgumentException("can only advance beyond current document: on docID=" + docID + " but targetDocID=" + targetDocID);
      }
      int readerIndex = ReaderUtil.subIndex(targetDocID, docStarts);
      if (readerIndex >= nextLeaf) {
        if (readerIndex == values.length) {
          currentValues = null;
          docID = NO_MORE_DOCS;
          return docID;
        }
        currentDocStart = docStarts[readerIndex];
        currentValues = values[readerIndex];
        nextLeaf = readerIndex+1;
      }
      int newDocID = currentValues.advance(targetDocID - currentDocStart);
      if (newDocID == NO_MORE_DOCS) {
        currentValues = null;
        return nextDoc();
      } else {
        docID = currentDocStart + newDocID;
        return docID;
      }
    }
    
    @Override
    public boolean advanceExact(int targetDocID) throws IOException {
      if (targetDocID < docID) {
        throw new IllegalArgumentException("can only advance beyond current document: on docID=" + docID + " but targetDocID=" + targetDocID);
      }
      int readerIndex = ReaderUtil.subIndex(targetDocID, docStarts);
      if (readerIndex >= nextLeaf) {
        if (readerIndex == values.length) {
          throw new IllegalArgumentException("Out of range: " + targetDocID);
        }
        currentDocStart = docStarts[readerIndex];
        currentValues = values[readerIndex];
        nextLeaf = readerIndex+1;
      }
      docID = targetDocID;
      if (currentValues == null) {
        return false;
      }
      return currentValues.advanceExact(targetDocID - currentDocStart);
    }
    
    @Override
    public int ordValue() throws IOException {
      return (int) mapping.getGlobalOrds(nextLeaf-1).get(currentValues.ordValue());
    }
 
    @Override
    public BytesRef lookupOrd(int ord) throws IOException {
      int subIndex = mapping.getFirstSegmentNumber(ord);
      int segmentOrd = (int) mapping.getFirstSegmentOrd(ord);
      return values[subIndex].lookupOrd(segmentOrd);
    }
 
    @Override
    public int getValueCount() {
      return (int) mapping.getValueCount();
    }

    @Override
    public long cost() {
      return totalCost;
    }
  }
  
  /** 
   * Implements MultiSortedSetDocValues over n subs, using an OrdinalMap 
   * @lucene.internal
   */
  public static class MultiSortedSetDocValues extends SortedSetDocValues {
    /** docbase for each leaf: parallel with {@link #values} */
    public final int docStarts[];
    /** leaf values */
    public final SortedSetDocValues values[];
    /** ordinal map mapping ords from <code>values</code> to global ord space */
    public final OrdinalMap mapping;
    private final long totalCost;

    private int nextLeaf;
    private SortedSetDocValues currentValues;
    private int currentDocStart;
    private int docID = -1;    

    /** Creates a new MultiSortedSetDocValues over <code>values</code> */
    public MultiSortedSetDocValues(SortedSetDocValues values[], int docStarts[], OrdinalMap mapping, long totalCost) throws IOException {
      assert docStarts.length == values.length + 1;
      this.values = values;
      this.docStarts = docStarts;
      this.mapping = mapping;
      this.totalCost = totalCost;
    }
    
    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextDoc() throws IOException {
      while (true) {
        while (currentValues == null) {
          if (nextLeaf == values.length) {
            docID = NO_MORE_DOCS;
            return docID;
          }
          currentDocStart = docStarts[nextLeaf];
          currentValues = values[nextLeaf];
          nextLeaf++;
        }

        int newDocID = currentValues.nextDoc();

        if (newDocID == NO_MORE_DOCS) {
          currentValues = null;
          continue;
        } else {
          docID = currentDocStart + newDocID;
          return docID;
        }
      }
    }

    @Override
    public int advance(int targetDocID) throws IOException {
      if (targetDocID <= docID) {
        throw new IllegalArgumentException("can only advance beyond current document: on docID=" + docID + " but targetDocID=" + targetDocID);
      }
      int readerIndex = ReaderUtil.subIndex(targetDocID, docStarts);
      if (readerIndex >= nextLeaf) {
        if (readerIndex == values.length) {
          currentValues = null;
          docID = NO_MORE_DOCS;
          return docID;
        }
        currentDocStart = docStarts[readerIndex];
        currentValues = values[readerIndex];
        nextLeaf = readerIndex+1;
      }
      int newDocID = currentValues.advance(targetDocID - currentDocStart);
      if (newDocID == NO_MORE_DOCS) {
        currentValues = null;
        return nextDoc();
      } else {
        docID = currentDocStart + newDocID;
        return docID;
      }
    }

    @Override
    public boolean advanceExact(int targetDocID) throws IOException {
      if (targetDocID < docID) {
        throw new IllegalArgumentException("can only advance beyond current document: on docID=" + docID + " but targetDocID=" + targetDocID);
      }
      int readerIndex = ReaderUtil.subIndex(targetDocID, docStarts);
      if (readerIndex >= nextLeaf) {
        if (readerIndex == values.length) {
          throw new IllegalArgumentException("Out of range: " + targetDocID);
        }
        currentDocStart = docStarts[readerIndex];
        currentValues = values[readerIndex];
        nextLeaf = readerIndex+1;
      }
      docID = targetDocID;
      if (currentValues == null) {
        return false;
      }
      return currentValues.advanceExact(targetDocID - currentDocStart);
    }

    @Override
    public long nextOrd() throws IOException {
      long segmentOrd = currentValues.nextOrd();
      if (segmentOrd == NO_MORE_ORDS) {
        return segmentOrd;
      } else {
        return mapping.getGlobalOrds(nextLeaf-1).get(segmentOrd);
      }
    }

    @Override
    public BytesRef lookupOrd(long ord) throws IOException {
      int subIndex = mapping.getFirstSegmentNumber(ord);
      long segmentOrd = mapping.getFirstSegmentOrd(ord);
      return values[subIndex].lookupOrd(segmentOrd);
    }
 
    @Override
    public long getValueCount() {
      return mapping.getValueCount();
    }

    @Override
    public long cost() {
      return totalCost;
    }
  }
}
