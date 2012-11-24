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
import java.util.Comparator;

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

// nocommit need marker interface?
public abstract class SortedDocValues extends BinaryDocValues {
  // nocommit throws IOE or not?
  public abstract int getOrd(int docID);

  // nocommit throws IOE or not?
  public abstract void lookupOrd(int ord, BytesRef result);

  // nocommit throws IOE or not?
  // nocommit .getUniqueValueCount?
  public abstract int getValueCount();

  @Override
  public void get(int docID, BytesRef result) {
    int ord = getOrd(docID);
    if (ord == -1) {
      // nocommit what to do ... maybe we need to return
      // BytesRef?
      throw new IllegalArgumentException("doc has no value");
    }
    lookupOrd(ord, result);
  }

  public TermsEnum getTermsEnum() {
    // nocommit who tests this base impl ...
    // Default impl just uses the existing API; subclasses
    // can specialize:
    return new TermsEnum() {
      private int currentOrd = -1;

      private final BytesRef term = new BytesRef();

      @Override
      public SeekStatus seekCeil(BytesRef text, boolean useCache /* ignored */) throws IOException {
        int low = 0;
        int high = getValueCount()-1;
        
        while (low <= high) {
          int mid = (low + high) >>> 1;
          seekExact(mid);
          int cmp = term.compareTo(text);

          if (cmp < 0)
            low = mid + 1;
          else if (cmp > 0)
            high = mid - 1;
          else {
            return SeekStatus.FOUND; // key found
          }
        }
        
        if (low == getValueCount()) {
          return SeekStatus.END;
        } else {
          seekExact(low);
          return SeekStatus.NOT_FOUND;
        }
      }

      @Override
      public void seekExact(long ord) throws IOException {
        assert ord >= 0 && ord < getValueCount();
        currentOrd = (int) ord;
        lookupOrd(currentOrd, term);
      }

      @Override
      public BytesRef next() throws IOException {
        currentOrd++;
        if (currentOrd >= getValueCount()) {
          return null;
        }
        lookupOrd(currentOrd, term);
        return term;
      }

      @Override
      public BytesRef term() throws IOException {
        return term;
      }

      @Override
      public long ord() throws IOException {
        return currentOrd;
      }

      @Override
      public int docFreq() {
        throw new UnsupportedOperationException();
      }

      @Override
      public long totalTermFreq() {
        return -1;
      }

      @Override
      public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public Comparator<BytesRef> getComparator() {
        return BytesRef.getUTF8SortedAsUnicodeComparator();
      }

      @Override
      public void seekExact(BytesRef term, TermState state) throws IOException {
        assert state != null && state instanceof OrdTermState;
        this.seekExact(((OrdTermState)state).ord);
      }

      @Override
      public TermState termState() throws IOException {
        OrdTermState state = new OrdTermState();
        state.ord = currentOrd;
        return state;
      }
    };
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

  // nocommit javadocs
  public int lookupTerm(BytesRef key, BytesRef spare) {
    // this special case is the reason that Arrays.binarySearch() isn't useful.
    if (key == null) {
      throw new IllegalArgumentException("key must not be null");
    }

    int low = 0;
    int high = getValueCount()-1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      lookupOrd(mid, spare);
      int cmp = spare.compareTo(key);

      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }

    return -(low + 1);  // key not found.
  }
}
