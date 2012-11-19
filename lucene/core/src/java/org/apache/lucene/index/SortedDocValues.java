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

import org.apache.lucene.util.BytesRef;

// nocommit need marker interface?
public abstract class SortedDocValues extends BinaryDocValues {
  // nocommit throws IOE or not?
  public abstract int getOrd(int docID);

  // nocommit throws IOE or not?
  public abstract void lookupOrd(int ord, BytesRef result);

  // nocommit throws IOE or not?
  public abstract int getValueCount();

  @Override
  public void get(int docID, BytesRef result) {
    int ord = getOrd(docID);
    lookupOrd(ord, result);
  }

  @Override
  public SortedDocValues newRAMInstance() {
    // nocommit optimize this
    // nocommit, see also BinaryDocValues nocommits
    final int maxDoc = size();
    final int maxLength = maxLength();
    final boolean fixedLength = isFixedLength();
    final int valueCount = getValueCount();
    // nocommit used packed ints and so on
    final byte[][] values = new byte[valueCount][];
    BytesRef scratch = new BytesRef();
    for(int ord=0;ord<values.length;ord++) {
      lookupOrd(ord, scratch);
      values[ord] = new byte[scratch.length];
      System.arraycopy(scratch.bytes, scratch.offset, values[ord], 0, scratch.length);
    }

    final int[] docToOrd = new int[maxDoc];
    for(int docID=0;docID<maxDoc;docID++) {
      docToOrd[docID] = getOrd(docID);
    }
    return new SortedDocValues() {

      @Override
      public int getOrd(int docID) {
        return docToOrd[docID];
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

      @Override
      public int size() {
        return maxDoc;
      }

      @Override
      public boolean isFixedLength() {
        return fixedLength;
      }

      @Override
      public int maxLength() {
        return maxLength;
      }

      @Override
      public SortedDocValues newRAMInstance() {
        return this; // see the nocommit in BinaryDocValues
      }
    };
  }

  // nocommit binary search lookup?
  public static class EMPTY extends SortedDocValues {
    private final int size;
    
    public EMPTY(int size) {
      this.size = size;
    }

    @Override
    public int getOrd(int docID) {
      return 0;
    }

    @Override
    public void lookupOrd(int ord, BytesRef result) {
      result.length = 0;
    }

    @Override
    public int getValueCount() {
      return 1;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public boolean isFixedLength() {
      return true;
    }

    @Override
    public int maxLength() {
      return 0;
    }
  }
}
