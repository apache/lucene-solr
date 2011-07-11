package org.apache.lucene.util;

/**
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
import java.util.BitSet;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * Stores and iterate on sorted integers in compressed form in RAM. <br>
 * The code for compressing the differences between ascending integers was
 * borrowed from {@link org.apache.lucene.store.IndexInput} and
 * {@link org.apache.lucene.store.IndexOutput}.
 * <p>
 * <b>NOTE:</b> this class assumes the stored integers are doc Ids (hence why it
 * extends {@link DocIdSet}). Therefore its {@link #iterator()} assumes {@link
 * DocIdSetIterator#NO_MORE_DOCS} can be used as sentinel. If you intent to use
 * this value, then make sure it's not used during search flow.
 *
 * @lucene.internal
 */
public class SortedVIntList extends DocIdSet {
  /** When a BitSet has fewer than 1 in BITS2VINTLIST_SIZE bits set,
   * a SortedVIntList representing the index numbers of the set bits
   * will be smaller than that BitSet.
   */
  final static int BITS2VINTLIST_SIZE = 8;

  private int size;
  private byte[] bytes;
  private int lastBytePos;
    
  /**
   *  Create a SortedVIntList from all elements of an array of integers.
   *
   * @param  sortedInts  A sorted array of non negative integers.
   */
  public SortedVIntList(int... sortedInts) {
    this(sortedInts, sortedInts.length);
  }

  /**
   * Create a SortedVIntList from an array of integers.
   * @param  sortedInts  An array of sorted non negative integers.
   * @param  inputSize   The number of integers to be used from the array.
   */
  public SortedVIntList(int[] sortedInts, int inputSize) {
    SortedVIntListBuilder builder = new SortedVIntListBuilder();
    for (int i = 0; i < inputSize; i++) {
      builder.addInt(sortedInts[i]);
    }
    builder.done();
  }

  /**
   * Create a SortedVIntList from a BitSet.
   * @param  bits  A bit set representing a set of integers.
   */
  public SortedVIntList(BitSet bits) {
    SortedVIntListBuilder builder = new SortedVIntListBuilder();
    int nextInt = bits.nextSetBit(0);
    while (nextInt != -1) {
      builder.addInt(nextInt);
      nextInt = bits.nextSetBit(nextInt + 1);
    }
    builder.done();
  }

  /**
   * Create a SortedVIntList.
   * @param  docIdSetIterator  An iterator providing document numbers as a set of integers.
   *                  This DocIdSetIterator is iterated completely when this constructor
   *                  is called and it must provide the integers in non
   *                  decreasing order.
   */
  public SortedVIntList(DocIdSetIterator docIdSetIterator) throws IOException {
    SortedVIntListBuilder builder = new SortedVIntListBuilder();
    int doc;
    while ((doc = docIdSetIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      builder.addInt(doc);
    }
    builder.done();
  }


  private class SortedVIntListBuilder {
    private int lastInt = 0;
    
    SortedVIntListBuilder() {
      initBytes();
      lastInt = 0;
    }

    void addInt(int nextInt) {
      int diff = nextInt - lastInt;
      if (diff < 0) {
        throw new IllegalArgumentException(
            "Input not sorted or first element negative.");
      }
  
      if ((lastBytePos + MAX_BYTES_PER_INT) > bytes.length) {
        // Biggest possible int does not fit.
        resizeBytes(ArrayUtil.oversize(lastBytePos + MAX_BYTES_PER_INT, 1));
      }
  
      // See org.apache.lucene.store.IndexOutput.writeVInt()
      while ((diff & ~VB1) != 0) { // The high bit of the next byte needs to be set.
        bytes[lastBytePos++] = (byte) ((diff & VB1) | ~VB1);
        diff >>>= BIT_SHIFT;
      }
      bytes[lastBytePos++] = (byte) diff; // Last byte, high bit not set.
      size++;
      lastInt = nextInt;
    }
    
    void done() {
      resizeBytes(lastBytePos);
    }
  }


  private void initBytes() {
    size = 0;
    bytes = new byte[128]; // initial byte size
    lastBytePos = 0;
  }

  private void resizeBytes(int newSize) {
    if (newSize != bytes.length) {
      byte[] newBytes = new byte[newSize];
      System.arraycopy(bytes, 0, newBytes, 0, lastBytePos);
      bytes = newBytes;
    }
  }

  private static final int VB1 = 0x7F;
  private static final int BIT_SHIFT = 7;
  private final int MAX_BYTES_PER_INT = (31 / BIT_SHIFT) + 1;

  /**
   * @return    The total number of sorted integers.
   */
  public int size() {
    return size;
  }

  /**
   * @return The size of the byte array storing the compressed sorted integers.
   */
  public int getByteSize() {
    return bytes.length;
  }

  /** This DocIdSet implementation is cacheable. */
  @Override
  public boolean isCacheable() {
    return true;
  }

  /**
   * @return    An iterator over the sorted integers.
   */
  @Override
  public DocIdSetIterator iterator() {
    return new DocIdSetIterator() {
      int bytePos = 0;
      int lastInt = 0;
      int doc = -1;
      
      private void advance() {
        // See org.apache.lucene.store.IndexInput.readVInt()
        byte b = bytes[bytePos++];
        lastInt += b & VB1;
        for (int s = BIT_SHIFT; (b & ~VB1) != 0; s += BIT_SHIFT) {
          b = bytes[bytePos++];
          lastInt += (b & VB1) << s;
        }
      }
      
      @Override
      public int docID() {
        return doc;
      }
      
      @Override
      public int nextDoc() {
        if (bytePos >= lastBytePos) {
          doc = NO_MORE_DOCS;
        } else {
          advance();
          doc = lastInt;
        }
        return doc;
      }
      
      @Override
      public int advance(int target) {
        while (bytePos < lastBytePos) {
          advance();
          if (lastInt >= target) {
            return doc = lastInt;
          }
        }
        return doc = NO_MORE_DOCS;
      }
      
    };
  }
}

