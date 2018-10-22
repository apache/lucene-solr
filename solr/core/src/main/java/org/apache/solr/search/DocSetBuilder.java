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
package org.apache.solr.search;

import java.io.IOException;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LSBRadixSorter;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Adapted from DocIdSetBuilder to build DocSets
 *
 * @lucene.internal
 */
public final class DocSetBuilder {

  private final int maxDoc;
  private final int threshold;

  private int[] buffer;
  private int pos;

  private FixedBitSet bitSet;


  public DocSetBuilder(int maxDoc, long costEst) {
    this.maxDoc = maxDoc;
    // For ridiculously small sets, we'll just use a sorted int[]
    // maxDoc >>> 7 is a good value if you want to save memory, lower values
    // such as maxDoc >>> 11 should provide faster building but at the expense
    // of using a full bitset even for quite sparse data
    this.threshold = (maxDoc >>> 7) + 4; // the +4 is for better testing on small indexes

    if (costEst > threshold) {
      bitSet = new FixedBitSet(maxDoc);
    } else {
      this.buffer = new int[Math.max((int)costEst,1)];
    }
  }

  private void upgradeToBitSet() {
    assert bitSet == null;
    bitSet = new FixedBitSet(maxDoc);
    for (int i = 0; i < pos; ++i) {
      bitSet.set(buffer[i]);
    }
    this.buffer = null;
    this.pos = 0;
  }

  private void growBuffer(int minSize) {
    if (minSize < buffer.length) return;

    int newSize = buffer.length;
    while (newSize < minSize) {
      newSize = newSize << 1;
    }
    newSize = Math.min(newSize, threshold);

    int[] newBuffer = new int[newSize];
    System.arraycopy(buffer, 0, newBuffer, 0, pos);
    buffer = newBuffer;
  }

  public void add(DocIdSetIterator iter, int base) throws IOException {
    grow((int) Math.min(Integer.MAX_VALUE, iter.cost()));

    if (bitSet != null) {
      add(bitSet, iter, base);
    } else {
      while (true) {
        for (int i = pos; i < buffer.length; ++i) {
          final int doc = iter.nextDoc();
          if (doc == DocIdSetIterator.NO_MORE_DOCS) {
            pos = i; // update pos
            return;
          }
          buffer[i] = doc + base;  // using the loop counter may help with removal of bounds checking
        }

        pos = buffer.length; // update pos
        if (pos + 1 >= threshold) {
          break;
        }

        growBuffer(pos + 1);
      }

      upgradeToBitSet();
      add(bitSet, iter, base);
    }
  }


  public static void add(FixedBitSet bitSet, DocIdSetIterator iter, int base) throws IOException {
    for (int doc = iter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iter.nextDoc()) {
      bitSet.set(doc + base);
    }
  }

  /** Returns the number of terms visited */
  public int add(TermsEnum te, int base) throws IOException {
    PostingsEnum postings = null;

    int termCount = 0;
    for(;;) {
      BytesRef term = te.next();
      if (term == null) break;
      termCount++;
      postings = te.postings(postings, PostingsEnum.NONE);
      add(postings, base);
    }

    return termCount;
  }


  public void grow(int numDocs) {
    if (bitSet == null) {
      final long newLength = pos + numDocs;
      if (newLength < threshold) {
        growBuffer((int) newLength);
      } else {
        upgradeToBitSet();
      }
    }
  }


  public void add(int doc) {
    if (bitSet != null) {
      bitSet.set(doc);
    } else {
      if (pos >= buffer.length) {
        if (pos + 1 >= threshold) {
          upgradeToBitSet();
          bitSet.set(doc);
          return;
        }
        growBuffer(pos + 1);
      }
      buffer[pos++] = doc;
    }
  }

  private static int dedup(int[] arr, int length, FixedBitSet acceptDocs) {
    int pos = 0;
    int previous = -1;
    for (int i = 0; i < length; ++i) {
      final int value = arr[i];
      // assert value >= previous;
      if (value != previous && (acceptDocs == null || acceptDocs.get(value))) {
        arr[pos++] = value;
        previous = value;
      }
    }
    return pos;
  }



  public DocSet build(FixedBitSet filter) {
    if (bitSet != null) {
      if (filter != null) {
        bitSet.and(filter);
      }
      return new BitDocSet(bitSet);
      // TODO - if this set will be cached, should we make it smaller if it's below DocSetUtil.smallSetSize?
    } else {
      LSBRadixSorter sorter = new LSBRadixSorter();
      sorter.sort(PackedInts.bitsRequired(maxDoc - 1), buffer, pos);
      final int l = dedup(buffer, pos, filter);
      assert l <= pos;
      return new SortedIntDocSet(buffer, l);  // TODO: have option to not shrink in the future if it will be a temporary set
    }
  }

  /** Only use this if you know there were no duplicates and that docs were collected in-order! */
  public DocSet buildUniqueInOrder(FixedBitSet filter) {
    if (bitSet != null) {
      if (filter != null) {
        bitSet.and(filter);
      }
      return new BitDocSet(bitSet);
    } else {
      // don't need to sort, but still need to remove non accepted docs
      int l = pos;
      if (filter != null) {
        l = dedup(buffer, pos, filter);
      }
      return new SortedIntDocSet(buffer, l);  // TODO: have option to not shrink in the future if it will be a temporary set
    }
  }

}

