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

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/** 
 * This class contains utility methods and constants for DocValues
 */
public final class DocValues {
  
  /* no instantiation */
  private DocValues() {}
  
  /** 
   * An empty BinaryDocValues which returns {@link BytesRef#EMPTY_BYTES} for every document 
   */
  public static final BinaryDocValues EMPTY_BINARY = new BinaryDocValues() {
    @Override
    public void get(int docID, BytesRef result) {
      result.bytes = BytesRef.EMPTY_BYTES;
      result.offset = 0;
      result.length = 0;
    }
  };

  /** 
   * An empty NumericDocValues which returns zero for every document 
   */
  public static final NumericDocValues EMPTY_NUMERIC = new NumericDocValues() {
    @Override
    public long get(int docID) {
      return 0;
    }
  };

  /** 
   * An empty SortedDocValues which returns {@link BytesRef#EMPTY_BYTES} for every document 
   */
  public static final SortedDocValues EMPTY_SORTED = new SortedDocValues() {
    @Override
    public int getOrd(int docID) {
      return -1;
    }

    @Override
    public void lookupOrd(int ord, BytesRef result) {
      result.bytes = BytesRef.EMPTY_BYTES;
      result.offset = 0;
      result.length = 0;
    }

    @Override
    public int getValueCount() {
      return 0;
    }
  };

  /** 
   * An empty SortedDocValues which returns {@link SortedSetDocValues#NO_MORE_ORDS} for every document 
   */
  public static final SortedSetDocValues EMPTY_SORTED_SET = new RandomAccessOrds() {

    @Override
    public long nextOrd() {
      return NO_MORE_ORDS;
    }

    @Override
    public void setDocument(int docID) {}

    @Override
    public void lookupOrd(long ord, BytesRef result) {
      throw new IndexOutOfBoundsException();
    }

    @Override
    public long getValueCount() {
      return 0;
    }

    @Override
    public long ordAt(int index) {
      throw new IndexOutOfBoundsException();
    }

    @Override
    public int cardinality() {
      return 0;
    }
  };
  
  /** 
   * Returns a multi-valued view over the provided SortedDocValues 
   */
  public static SortedSetDocValues singleton(SortedDocValues dv) {
    return new SingletonSortedSetDocValues(dv);
  }
  
  /** 
   * Returns a single-valued view of the SortedSetDocValues, if it was previously
   * wrapped with {@link #singleton}, or null. 
   */
  public static SortedDocValues unwrapSingleton(SortedSetDocValues dv) {
    if (dv instanceof SingletonSortedSetDocValues) {
      return ((SingletonSortedSetDocValues)dv).getSortedDocValues();
    } else {
      return null;
    }
  }
  
  /**
   * Returns a Bits representing all documents from <code>dv</code> that have a value.
   */
  public static Bits docsWithValue(final SortedDocValues dv, final int maxDoc) {
    return new Bits() {
      @Override
      public boolean get(int index) {
        return dv.getOrd(index) >= 0;
      }

      @Override
      public int length() {
        return maxDoc;
      }
    };
  }
  
  /**
   * Returns a Bits representing all documents from <code>dv</code> that have a value.
   */
  public static Bits docsWithValue(final SortedSetDocValues dv, final int maxDoc) {
    return new Bits() {
      @Override
      public boolean get(int index) {
        dv.setDocument(index);
        return dv.nextOrd() != SortedSetDocValues.NO_MORE_ORDS;
      }

      @Override
      public int length() {
        return maxDoc;
      }
    };
  }
  
  // some helpers, for transition from fieldcache apis.
  // as opposed to the AtomicReader apis (which must be strict for consistency), these are lenient
  
  /**
   * Returns NumericDocValues for the reader, or {@link #EMPTY_NUMERIC} if it has none. 
   */
  public static NumericDocValues getNumeric(AtomicReader in, String field) throws IOException {
    NumericDocValues dv = in.getNumericDocValues(field);
    if (dv == null) {
      return EMPTY_NUMERIC;
    } else {
      return dv;
    }
  }
  
  /**
   * Returns BinaryDocValues for the reader, or {@link #EMPTY_BINARY} if it has none. 
   */
  public static BinaryDocValues getBinary(AtomicReader in, String field) throws IOException {
    BinaryDocValues dv = in.getBinaryDocValues(field);
    if (dv == null) {
      dv = in.getSortedDocValues(field);
      if (dv == null) {
        return EMPTY_BINARY;
      }
    }
    return dv;
  }
  
  /**
   * Returns SortedDocValues for the reader, or {@link #EMPTY_SORTED} if it has none. 
   */
  public static SortedDocValues getSorted(AtomicReader in, String field) throws IOException {
    SortedDocValues dv = in.getSortedDocValues(field);
    if (dv == null) {
      return EMPTY_SORTED;
    } else {
      return dv;
    }
  }
  
  /**
   * Returns SortedSetDocValues for the reader, or {@link #EMPTY_SORTED_SET} if it has none. 
   */
  public static SortedSetDocValues getSortedSet(AtomicReader in, String field) throws IOException {
    SortedSetDocValues dv = in.getSortedSetDocValues(field);
    if (dv == null) {
      SortedDocValues sorted = in.getSortedDocValues(field);
      if (sorted == null) {
        return EMPTY_SORTED_SET;
      }
      return singleton(sorted);
    }
    return dv;
  }
  
  /**
   * Returns Bits for the reader, or {@link Bits} matching nothing if it has none. 
   */
  public static Bits getDocsWithField(AtomicReader in, String field) throws IOException {
    Bits dv = in.getDocsWithField(field);
    if (dv == null) {
      return new Bits.MatchNoBits(in.maxDoc());
    } else {
      return dv;
    }
  }
}
