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

public abstract class BinaryDocValues {

  public abstract void get(int docID, BytesRef result);

  public static final byte[] MISSING = new byte[0];
  
  public abstract int size();
  
  public abstract boolean isFixedLength();
  public abstract int maxLength();
  
  // nocommit: rethink this api? alternative is boolean on atomicreader...?
  // doc that the thing returned here must be thread safe...
  public BinaryDocValues newRAMInstance() {
    // TODO: optimize this default impl with e.g. isFixedLength/maxLength and so on
    // nocommit used packed ints/pagedbytes and so on
    final int maxDoc = size();
    final int maxLength = maxLength();
    final boolean fixedLength = isFixedLength();
    final byte[][] values = new byte[maxDoc][];
    BytesRef scratch = new BytesRef();
    for(int docID=0;docID<maxDoc;docID++) {
      get(docID, scratch);
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
      public BinaryDocValues newRAMInstance() {
        // nocommit: ugly, maybe throw exception instead?
        return this; 
      }
    };
  }
  
  public static class EMPTY extends BinaryDocValues {
    private final int size;
    
    public EMPTY(int size) {
      this.size = size;
    }
    
    @Override
    public void get(int docID, BytesRef result) {
      result.length = 0;
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
  };
}
