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

public abstract class SortedDocValues extends BinaryDocValues {
  public abstract int getOrd(int docID);

  public abstract void lookupOrd(int ord, BytesRef result);

  public abstract int getValueCount();

  @Override
  public void get(int docID, BytesRef result) {
    int ord = getOrd(docID);
    if (ord == -1) {
      result.bytes = MISSING;
      result.length = 0;
    } else {
      lookupOrd(ord, result);
    }
  }

  // nocommit make this final, and impl seekExact(term) to
  // fwd to lookupTerm

  // nocommit should we nuke this?  the iterator can be
  // efficiently built "on top" since ord is part of the
  // API?  why must it be impl'd here...?
  // SortedDocValuesTermsEnum.

  public static final SortedDocValues EMPTY = new SortedDocValues() {
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
  };

  /** If {@code key} exists, returns its ordinal, else
   *  returns {@code -insertionPoint-1}, like {@code
   *  Arrays.binarySearch}.
   *
   *  @param key Key to look up
   *  @param spare Spare BytesRef
   **/
  public int lookupTerm(BytesRef key, BytesRef spare) {

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
        // nocommit is this the right way... else caller can
        // pass this spare down to DiskDV, which will then
        // "use" our byte[] ...
        spare.bytes = BytesRef.EMPTY_BYTES;
        return mid; // key found
      }
    }

    // nocommit is this the right way...
    spare.bytes = BytesRef.EMPTY_BYTES;
    return -(low + 1);  // key not found.
  }
}
