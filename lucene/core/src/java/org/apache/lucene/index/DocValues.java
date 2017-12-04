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
import java.util.Arrays;

import org.apache.lucene.util.BytesRef;

/** 
 * This class contains utility methods and constants for DocValues
 */
public final class DocValues {

  /* no instantiation */
  private DocValues() {}

  /** 
   * An empty {@link BinaryDocValues} which returns no documents
   */
  public static final BinaryDocValues emptyBinary() {
    return new BinaryDocValues() {
      private int doc = -1;
      
      @Override
      public int advance(int target) {
        return doc = NO_MORE_DOCS;
      }
      
      @Override
      public boolean advanceExact(int target) throws IOException {
        doc = target;
        return false;
      }
      
      @Override
      public int docID() {
        return doc;
      }
      
      @Override
      public int nextDoc() {
        return doc = NO_MORE_DOCS;
      }
      
      @Override
      public long cost() {
        return 0;
      }

      @Override
      public BytesRef binaryValue() {
        assert false;
        return null;
      }
    };
  }

  /** 
   * An empty NumericDocValues which returns no documents
   */
  public static final NumericDocValues emptyNumeric() {
    return new NumericDocValues() {
      private int doc = -1;
      
      @Override
      public int advance(int target) {
        return doc = NO_MORE_DOCS;
      }
      
      @Override
      public boolean advanceExact(int target) throws IOException {
        doc = target;
        return false;
      }
      
      @Override
      public int docID() {
        return doc;
      }
      
      @Override
      public int nextDoc() {
        return doc = NO_MORE_DOCS;
      }
      
      @Override
      public long cost() {
        return 0;
      }

      @Override
      public long longValue() {
        assert false;
        return 0;
      }
    };
  }

  /** 
   * An empty SortedDocValues which returns {@link BytesRef#EMPTY_BYTES} for every document 
   */
  public static final LegacySortedDocValues emptyLegacySorted() {
    final BytesRef empty = new BytesRef();
    return new LegacySortedDocValues() {
      @Override
      public int getOrd(int docID) {
        return -1;
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        return empty;
      }

      @Override
      public int getValueCount() {
        return 0;
      }
    };
  }

  /** 
   * An empty SortedDocValues which returns {@link BytesRef#EMPTY_BYTES} for every document
   */
  public static final SortedDocValues emptySorted() {
    final BytesRef empty = new BytesRef();
    return new SortedDocValues() {
      
      private int doc = -1;
      
      @Override
      public int advance(int target) {
        return doc = NO_MORE_DOCS;
      }
      
      @Override
      public boolean advanceExact(int target) throws IOException {
        doc = target;
        return false;
      }
      
      @Override
      public int docID() {
        return doc;
      }
      
      @Override
      public int nextDoc() {
        return doc = NO_MORE_DOCS;
      }
      
      @Override
      public long cost() {
        return 0;
      }

      @Override
      public int ordValue() {
        assert false;
        return -1;
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        return empty;
      }

      @Override
      public int getValueCount() {
        return 0;
      }
    };
  }

  /**
   * An empty SortedNumericDocValues which returns zero values for every document 
   */
  public static final SortedNumericDocValues emptySortedNumeric(int maxDoc) {
    return new SortedNumericDocValues() {
      
      private int doc = -1;
      
      @Override
      public int advance(int target) {
        return doc = NO_MORE_DOCS;
      }
      
      @Override
      public boolean advanceExact(int target) throws IOException {
        doc = target;
        return false;
      }
      
      @Override
      public int docID() {
        return doc;
      }
      
      @Override
      public int nextDoc() {
        return doc = NO_MORE_DOCS;
      }
      
      @Override
      public long cost() {
        return 0;
      }

      @Override
      public int docValueCount() {
        throw new IllegalStateException();
      }

      @Override
      public long nextValue() {
        throw new IllegalStateException();
      }
    };
  }

  /** 
   * An empty SortedDocValues which returns {@link BytesRef#EMPTY_BYTES} for every document
   */
  public static final SortedSetDocValues emptySortedSet() {
    final BytesRef empty = new BytesRef();
    return new SortedSetDocValues() {
      
      private int doc = -1;
      
      @Override
      public int advance(int target) {
        return doc = NO_MORE_DOCS;
      }
      
      @Override
      public boolean advanceExact(int target) throws IOException {
        doc = target;
        return false;
      }
      
      @Override
      public int docID() {
        return doc;
      }
      
      @Override
      public int nextDoc() {
        return doc = NO_MORE_DOCS;
      }
      
      @Override
      public long cost() {
        return 0;
      }

      @Override
      public long nextOrd() {
        assert false;
        return NO_MORE_ORDS;
      }

      @Override
      public BytesRef lookupOrd(long ord) {
        return empty;
      }

      @Override
      public long getValueCount() {
        return 0;
      }
    };
  }

  /** 
   * Returns a multi-valued view over the provided SortedDocValues
   */
  public static SortedSetDocValues singleton(SortedDocValues dv) {
    return new SingletonSortedSetDocValues(dv);
  }
  
  /** 
   * Returns a single-valued view of the SortedSetDocValues, if it was previously
   * wrapped with {@link #singleton(SortedDocValues)}, or null.
   */
  public static SortedDocValues unwrapSingleton(SortedSetDocValues dv) {
    if (dv instanceof SingletonSortedSetDocValues) {
      return ((SingletonSortedSetDocValues)dv).getSortedDocValues();
    } else {
      return null;
    }
  }
  
  /** 
   * Returns a single-valued view of the SortedNumericDocValues, if it was previously
   * wrapped with {@link #singleton(NumericDocValues)}, or null.
   */
  public static NumericDocValues unwrapSingleton(SortedNumericDocValues dv) {
    if (dv instanceof SingletonSortedNumericDocValues) {
      return ((SingletonSortedNumericDocValues)dv).getNumericDocValues();
    } else {
      return null;
    }
  }
  
  /**
   * Returns a multi-valued view over the provided NumericDocValues
   */
  public static SortedNumericDocValues singleton(NumericDocValues dv) {
    return new SingletonSortedNumericDocValues(dv);
  }
  
  // some helpers, for transition from fieldcache apis.
  // as opposed to the LeafReader apis (which must be strict for consistency), these are lenient
  
  // helper method: to give a nice error when LeafReader.getXXXDocValues returns null.
  private static void checkField(LeafReader in, String field, DocValuesType... expected) {
    FieldInfo fi = in.getFieldInfos().fieldInfo(field);
    if (fi != null) {
      DocValuesType actual = fi.getDocValuesType();
      throw new IllegalStateException("unexpected docvalues type " + actual + 
                                        " for field '" + field + "' " +
                                        (expected.length == 1 
                                        ? "(expected=" + expected[0]
                                        : "(expected one of " + Arrays.toString(expected)) + "). " +
                                        "Re-index with correct docvalues type.");
    }
  }
  
  /**
   * Returns NumericDocValues for the field, or {@link #emptyNumeric()} if it has none.
   * @return docvalues instance, or an empty instance if {@code field} does not exist in this reader.
   * @throws IllegalStateException if {@code field} exists, but was not indexed with docvalues.
   * @throws IllegalStateException if {@code field} has docvalues, but the type is not {@link DocValuesType#NUMERIC}.
   * @throws IOException if an I/O error occurs.
   */
  public static NumericDocValues getNumeric(LeafReader reader, String field) throws IOException {
    NumericDocValues dv = reader.getNumericDocValues(field);
    if (dv == null) {
      checkField(reader, field, DocValuesType.NUMERIC);
      return emptyNumeric();
    } else {
      return dv;
    }
  }
  
  /**
   * Returns BinaryDocValues for the field, or {@link #emptyBinary} if it has none.
   * @return docvalues instance, or an empty instance if {@code field} does not exist in this reader.
   * @throws IllegalStateException if {@code field} exists, but was not indexed with docvalues.
   * @throws IllegalStateException if {@code field} has docvalues, but the type is not {@link DocValuesType#BINARY}
   *                               or {@link DocValuesType#SORTED}.
   * @throws IOException if an I/O error occurs.
   */
  public static BinaryDocValues getBinary(LeafReader reader, String field) throws IOException {
    BinaryDocValues dv = reader.getBinaryDocValues(field);
    if (dv == null) {
      dv = reader.getSortedDocValues(field);
      if (dv == null) {
        checkField(reader, field, DocValuesType.BINARY, DocValuesType.SORTED);
        return emptyBinary();
      }
    }
    return dv;
  }
  
  /**
   * Returns SortedDocValues for the field, or {@link #emptySorted} if it has none.
   * @return docvalues instance, or an empty instance if {@code field} does not exist in this reader.
   * @throws IllegalStateException if {@code field} exists, but was not indexed with docvalues.
   * @throws IllegalStateException if {@code field} has docvalues, but the type is not {@link DocValuesType#SORTED}.
   * @throws IOException if an I/O error occurs.
   */
  public static SortedDocValues getSorted(LeafReader reader, String field) throws IOException {
    SortedDocValues dv = reader.getSortedDocValues(field);
    if (dv == null) {
      checkField(reader, field, DocValuesType.SORTED);
      return emptySorted();
    } else {
      return dv;
    }
  }
  
  /**
   * Returns SortedNumericDocValues for the field, or {@link #emptySortedNumeric} if it has none.
   * @return docvalues instance, or an empty instance if {@code field} does not exist in this reader.
   * @throws IllegalStateException if {@code field} exists, but was not indexed with docvalues.
   * @throws IllegalStateException if {@code field} has docvalues, but the type is not {@link DocValuesType#SORTED_NUMERIC}
   *                               or {@link DocValuesType#NUMERIC}.
   * @throws IOException if an I/O error occurs.
   */
  public static SortedNumericDocValues getSortedNumeric(LeafReader reader, String field) throws IOException {
    SortedNumericDocValues dv = reader.getSortedNumericDocValues(field);
    if (dv == null) {
      NumericDocValues single = reader.getNumericDocValues(field);
      if (single == null) {
        checkField(reader, field, DocValuesType.SORTED_NUMERIC, DocValuesType.NUMERIC);
        return emptySortedNumeric(reader.maxDoc());
      }
      return singleton(single);
    }
    return dv;
  }
  
  /**
   * Returns SortedSetDocValues for the field, or {@link #emptySortedSet} if it has none. 
   * @return docvalues instance, or an empty instance if {@code field} does not exist in this reader.
   * @throws IllegalStateException if {@code field} exists, but was not indexed with docvalues.
   * @throws IllegalStateException if {@code field} has docvalues, but the type is not {@link DocValuesType#SORTED_SET}
   *                               or {@link DocValuesType#SORTED}.
   * @throws IOException if an I/O error occurs.
   */
  public static SortedSetDocValues getSortedSet(LeafReader reader, String field) throws IOException {
    SortedSetDocValues dv = reader.getSortedSetDocValues(field);
    if (dv == null) {
      SortedDocValues sorted = reader.getSortedDocValues(field);
      if (sorted == null) {
        checkField(reader, field, DocValuesType.SORTED, DocValuesType.SORTED_SET);
        return emptySortedSet();
      }
      dv = singleton(sorted);
    }
    return dv;
  }

  /**
   * Returns {@code true} if the specified docvalues fields have not been updated
   */
  public static boolean isCacheable(LeafReaderContext ctx, String... fields) {
    for (String field : fields) {
      FieldInfo fi = ctx.reader().getFieldInfos().fieldInfo(field);
      if (fi != null && fi.getDocValuesGen() > -1)
        return false;
    }
    return true;
  }
}
