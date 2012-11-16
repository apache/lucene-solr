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

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;

public abstract class SimpleDVProducer implements Closeable {

  private final int maxDoc;

  protected SimpleDVProducer(int maxDoc) {
    // nocommit kinda messy?
    this.maxDoc = maxDoc;
  }

  public abstract NumericDocValues getDirectNumeric(FieldInfo field) throws IOException;

  /** Loads all values into RAM. */
  public NumericDocValues getNumeric(FieldInfo field) throws IOException {
    NumericDocValues source = getDirectNumeric(field);
    // nocommit more ram efficient?
    final long[] values = new long[maxDoc];
    for(int docID=0;docID<maxDoc;docID++) {
      values[docID] = source.get(docID);
    }
    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        return values[docID];
      }
    };
  }

  public abstract BinaryDocValues getDirectBinary(FieldInfo field) throws IOException;

  /** Loads all values into RAM. */
  public BinaryDocValues getBinary(FieldInfo field) throws IOException {
    
    BinaryDocValues source = getDirectBinary(field);

    // nocommit more ram efficient
    final byte[][] values = new byte[maxDoc][];
    BytesRef scratch = new BytesRef();
    for(int docID=0;docID<maxDoc;docID++) {
      source.get(docID, scratch);
      values[docID] = new byte[scratch.length];
      System.arraycopy(scratch.bytes, scratch.offset, values[docID], 0, scratch.length);
    }

    return new BinaryDocValues() {
      @Override
      public void get(int docID, BytesRef result) {
        result.bytes = values[docID];
        result.offset = 0;
        result.length = result.bytes.length;
      }
    };
  }

  public abstract SortedDocValues getDirectSorted(FieldInfo field) throws IOException;

  /** Loads all values into RAM. */
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    SortedDocValues source = getDirectSorted(field);
    final int valueCount = source.getValueCount();
    final byte[][] values = new byte[valueCount][];
    BytesRef scratch = new BytesRef();
    for(int ord=0;ord<valueCount;ord++) {
      source.lookupOrd(ord, scratch);
      values[ord] = new byte[scratch.length];
      System.arraycopy(scratch.bytes, scratch.offset, values[ord], 0, scratch.length);
    }

    final int[] ords = new int[maxDoc];
    for(int docID=0;docID<maxDoc;docID++) {
      ords[docID] = source.getOrd(docID);
    }

    return new SortedDocValues() {
      @Override
      public int getOrd(int docID) {
        return ords[docID];
      }

      @Override
      public void lookupOrd(int ord, BytesRef result) {
        result.bytes = values[ord];
        result.offset = 0;
        result.length = result.bytes.length;
      }

      @Override
      public int getValueCount() {
        return valueCount;
      }
    };
  }
}
