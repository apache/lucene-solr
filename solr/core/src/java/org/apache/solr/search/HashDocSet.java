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

import java.util.Collection;
import java.util.Collections;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.RamUsageEstimator;


/**
 * <code>HashDocSet</code> represents an unordered set of Lucene Document Ids
 * using a primitive int hash table.  It can be a better choice if there are few docs
 * in the set because it takes up less memory and is faster to iterate and take
 * set intersections.
 *
 *
 * @since solr 0.9
 */
@Deprecated // see SOLR-14256
public final class HashDocSet extends DocSetBase {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(HashDocSet.class) + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;

  /** Default load factor to use for HashDocSets.  We keep track of the inverse
   *  since multiplication is so much faster than division.  The default
   *  is 1.0f / 0.75f
   */
  static float DEFAULT_INVERSE_LOAD_FACTOR = 1.0f /0.75f;

  // public final static int MAX_SIZE = SolrConfig.config.getInt("//HashDocSet/@maxSize",-1);


  // lucene docs are numbered from 0, so a neg number must be used for missing.
  // an alternative to having to init the array to EMPTY at the start is
  //
  private final static int EMPTY=-1;
  private final int[] table;
  private final int size;
  private final int mask;

  public HashDocSet(HashDocSet set) {
    this.table = set.table.clone();
    this.size = set.size;
    this.mask = set.mask;
  }

  /** Create a HashDocSet from a list of *unique* ids */
  public HashDocSet(int[] docs, int offset, int len) {
    this(docs, offset, len, DEFAULT_INVERSE_LOAD_FACTOR);
  }

  /** Create a HashDocSet from a list of *unique* ids */  
  public HashDocSet(int[] docs, int offset, int len, float inverseLoadFactor) {
    int tsize = Math.max(BitUtil.nextHighestPowerOfTwo(len), 1);
    if (tsize < len * inverseLoadFactor) {
      tsize <<= 1;
    }

    mask=tsize-1;

    table = new int[tsize];
    // (for now) better then: Arrays.fill(table, EMPTY);
    // https://issues.apache.org/jira/browse/SOLR-390
    for (int i=tsize-1; i>=0; i--) table[i]=EMPTY;

    int end = offset + len;
    for (int i=offset; i<end; i++) {
      put(docs[i]);
    }

    size = len;
  }

  void put(int doc) {
    int s = doc & mask;
    while (table[s]!=EMPTY) {
      // Adding an odd number to this power-of-two hash table is
      // guaranteed to do a full traversal, so instead of re-hashing
      // we jump straight to a "linear" traversal.
      // The key is that we provide many different ways to do the
      // traversal (tablesize/2) based on the last hash code (the doc).
      // Rely on loop invariant code motion to eval ((doc>>7)|1) only once.
      // otherwise, we would need to pull the first case out of the loop.
      s = (s + ((doc>>7)|1)) & mask;
    }
    table[s]=doc;
  }

  @Override
  public boolean exists(int doc) {
    int s = doc & mask;
    for(;;) {
      int v = table[s];
      if (v==EMPTY) return false;
      if (v==doc) return true;
      // see put() for algorithm details.
      s = (s + ((doc>>7)|1)) & mask;
    }
  }


  @Override
  public int size() {
    return size;
  }

  @Override
  public DocIterator iterator() {
    return new DocIterator() {
      int pos=0;
      int doc;
      { goNext(); }

      @Override
      public boolean hasNext() {
        return pos < table.length;
      }

      @Override
      public Integer next() {
        return nextDoc();
      }

      @Override
      public void remove() {
      }

      void goNext() {
        while (pos<table.length && table[pos]==EMPTY) pos++;
      }

      // modify to return -1 at end of iteration?
      @Override
      public int nextDoc() {
        int doc = table[pos];
        pos++;
        goNext();
        return doc;
      }

      @Override
      public float score() {
        return 0.0f;
      }
    };
  }

  @Override
  public DocSet intersection(DocSet other) {
   if (other instanceof HashDocSet) {
     // set "a" to the smallest doc set for the most efficient
     // intersection.
     final HashDocSet a = size()<=other.size() ? this : (HashDocSet)other;
     final HashDocSet b = size()<=other.size() ? (HashDocSet)other : this;

     int[] result = new int[a.size()];
     int resultCount=0;
     for (int i=0; i<a.table.length; i++) {
       int id=a.table[i];
       if (id >= 0 && b.exists(id)) {
         result[resultCount++]=id;
       }
     }
     return new HashDocSet(result,0,resultCount);

   } else {

     int[] result = new int[size()];
     int resultCount=0;
     for (int i=0; i<table.length; i++) {
       int id=table[i];
       if (id >= 0 && other.exists(id)) {
         result[resultCount++]=id;
       }
     }
     return new HashDocSet(result,0,resultCount);
   }

  }

  @Override
  public int intersectionSize(DocSet other) {
   if (other instanceof HashDocSet) {
     // set "a" to the smallest doc set for the most efficient
     // intersection.
     final HashDocSet a = size()<=other.size() ? this : (HashDocSet)other;
     final HashDocSet b = size()<=other.size() ? (HashDocSet)other : this;

     int resultCount=0;
     for (int i=0; i<a.table.length; i++) {
       int id=a.table[i];
       if (id >= 0 && b.exists(id)) {
         resultCount++;
       }
     }
     return resultCount;
   } else {
     int resultCount=0;
     for (int i=0; i<table.length; i++) {
       int id=table[i];
       if (id >= 0 && other.exists(id)) {
         resultCount++;
       }
     }
     return resultCount;
   }

  }

  @Override
  public boolean intersects(DocSet other) {
   if (other instanceof HashDocSet) {
     // set "a" to the smallest doc set for the most efficient
     // intersection.
     final HashDocSet a = size()<=other.size() ? this : (HashDocSet)other;
     final HashDocSet b = size()<=other.size() ? (HashDocSet)other : this;

     for (int i=0; i<a.table.length; i++) {
       int id=a.table[i];
       if (id >= 0 && b.exists(id)) {
         return true;
       }
     }
     return false;
   } else {
     for (int i=0; i<table.length; i++) {
       int id=table[i];
       if (id >= 0 && other.exists(id)) {
         return true;
       }
     }
     return false;
   }
  }

  @Override
  public DocSet andNot(DocSet other) {
    int[] result = new int[size()];
    int resultCount=0;

    for (int i=0; i<table.length; i++) {
      int id=table[i];
      if (id >= 0 && !other.exists(id)) {
        result[resultCount++]=id;
      }
    }
    return new HashDocSet(result,0,resultCount);
  }

  @Override
  public DocSet union(DocSet other) {
   if (other instanceof HashDocSet) {
     // set "a" to the smallest doc set
     final HashDocSet a = size()<=other.size() ? this : (HashDocSet)other;
     final HashDocSet b = size()<=other.size() ? (HashDocSet)other : this;

     int[] result = new int[a.size()+b.size()];
     int resultCount=0;
     // iterate over the largest table first, adding w/o checking.
     for (int i=0; i<b.table.length; i++) {
       int id=b.table[i];
       if (id>=0) result[resultCount++]=id;
     }

     // now iterate over smaller set, adding all not already in larger set.
     for (int i=0; i<a.table.length; i++) {
       int id=a.table[i];
       if (id>=0 && !b.exists(id)) result[resultCount++]=id;
     }

     return new HashDocSet(result,0,resultCount);
   } else {
     return other.union(this);
   }
  }

  @Override
  public HashDocSet clone() {
    return new HashDocSet(this);
  }

  // don't implement andNotSize() and unionSize() on purpose... they are implemented
  // in BaseDocSet in terms of intersectionSize().


  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + (table.length<<2);
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }
}
