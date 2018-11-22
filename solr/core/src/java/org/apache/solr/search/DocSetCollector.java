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
import java.util.ArrayList;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.FixedBitSet;

/**
 *
 */

public class DocSetCollector extends SimpleCollector {
  int pos=0;
  FixedBitSet bits;
  final int maxDoc;
  final int smallSetSize;
  int base;

  // in case there aren't that many hits, we may not want a very sparse
  // bit array.  Optimistically collect the first few docs in an array
  // in case there are only a few.
  final ExpandingIntArray scratch;

  public DocSetCollector(int maxDoc) {
    this(DocSetUtil.smallSetSize(maxDoc), maxDoc);
  }

  public DocSetCollector(int smallSetSize, int maxDoc) {
    this.smallSetSize = smallSetSize;
    this.maxDoc = maxDoc;
    this.scratch = new ExpandingIntArray(smallSetSize);
  }

  @Override
  public void collect(int doc) throws IOException {
    doc += base;
    // optimistically collect the first docs in an array
    // in case the total number will be small enough to represent
    // as a small set like SortedIntDocSet instead...
    // Storing in this array will be quicker to convert
    // than scanning through a potentially huge bit vector.
    // FUTURE: when search methods all start returning docs in order, maybe
    // we could have a ListDocSet() and use the collected array directly.
    if (pos < smallSetSize) {
      scratch.add(pos, doc);
    } else {
      // this conditional could be removed if BitSet was preallocated, but that
      // would take up more memory, and add more GC time...
      if (bits==null) bits = new FixedBitSet(maxDoc);
      bits.set(doc);
    }

    pos++;
  }

  /** The number of documents that have been collected */
  public int size() {
    return pos;
  }

  public DocSet getDocSet() {
    if (pos<=scratch.size()) {
      // assumes docs were collected in sorted order!
      return new SortedIntDocSet(scratch.toArray(), pos);
//    } else if (pos == maxDoc) {
//      return new MatchAllDocSet(maxDoc);  // a bunch of code currently relies on BitDocSet (either explicitly, or implicitly for performance)
    } else {
      // set the bits for ids that were collected in the array
      scratch.copyTo(bits);
      return new BitDocSet(bits,pos);
    }
  }

  @Override
  public void setScorer(Scorable scorer) throws IOException {
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    this.base = context.docBase;
  }

  protected static class ExpandingIntArray {
    private static final int[] EMPTY = new int[0];
    private int[] currentAddArray = null;
    private int indexForNextAddInCurrentAddArray = 0;
    private int size = 0;
    private final int smallSetSize;
    private ArrayList<int[]> arrays;

    public ExpandingIntArray(int smallSetSize) {
      this.smallSetSize = smallSetSize;
      this.currentAddArray = EMPTY;
    }

    private void addNewArray() {
      int arrSize = Math.max(10, currentAddArray.length << 1);
      arrSize = Math.min(arrSize, smallSetSize - size); // max out at the smallSetSize
      this.currentAddArray = new int[arrSize];
      if (arrays == null) {
        arrays = new ArrayList<>();
      }
      arrays.add(this.currentAddArray);
      indexForNextAddInCurrentAddArray = 0;
      // System.out.println("### ALLOCATED " + this + " " + arrSize + " smallSetSize="+smallSetSize + " left=" + (smallSetSize-size));
    }

    public void add(int index, int value) {
      // assert index == size; // only appending is supported
      if (indexForNextAddInCurrentAddArray >= currentAddArray.length) {
        addNewArray();
      }
      currentAddArray[indexForNextAddInCurrentAddArray++] = value;
      size++;
    }

    public void copyTo(FixedBitSet bits) {
      if (size > 0) {
        int resultPos = 0;
        for (int i = 0; i < arrays.size(); i++) {
          int[] srcArray = arrays.get(i);
          int intsToCopy = (i < (arrays.size() - 1)) ? srcArray.length : indexForNextAddInCurrentAddArray;
          for (int j = 0; j < intsToCopy; j++) {
            bits.set(srcArray[j]);
          }
          resultPos += intsToCopy;
        }
        assert resultPos == size;
      }
    }

    public int[] toArray() {
      int[] result = new int[size];
      if (size > 0) {
        int resultPos = 0;
        for (int i = 0; i < arrays.size(); i++) {
          int[] srcArray = arrays.get(i);
          int intsToCopy = (i < (arrays.size() - 1)) ? srcArray.length : indexForNextAddInCurrentAddArray;
          System.arraycopy(srcArray, 0, result, resultPos, intsToCopy);
          resultPos += intsToCopy;
        }
        assert resultPos == size;
      }
      return result;
    }

    public int size() {
      return size;
    }
  }
}
