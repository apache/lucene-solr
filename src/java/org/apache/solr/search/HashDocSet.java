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

package org.apache.solr.search;

import org.apache.solr.util.BitUtil;


/**
 * <code>HashDocSet</code> represents an unordered set of Lucene Document Ids
 * using a primitive int hash table.  It can be a better choice if there are few docs
 * in the set because it takes up less memory and is faster to iterate and take
 * set intersections.
 *
 * @author yonik
 * @version $Id$
 * @since solr 0.9
 */
public final class HashDocSet extends DocSetBase {
  // final static float inverseLoadfactor = 1.0f / SolrConfig.config.getFloat("//HashDocSet/@loadFactor",0.75f);
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
  private final int tablesize;
  private final int[] table;
  private final int size;

  private final int mask;

  public HashDocSet(int[] docs, int offset, int len) {
    this(docs, offset, len, DEFAULT_INVERSE_LOAD_FACTOR);
  }

  public HashDocSet(int[] docs, int offset, int len, float inverseLoadFactor) {
    int tsize = Math.max(BitUtil.nextHighestPowerOfTwo(len), 1);
    if (tsize < len * inverseLoadFactor) {
      tsize <<= 1;
    }

    tablesize = tsize;
    mask=tablesize-1;

    table = new int[tablesize];
    for (int i=0; i<tablesize; i++) table[i]=EMPTY;

    for (int i=offset; i<len; i++) {
      put(docs[i]);
    }

    size = len;
  }

  void put(int doc) {
    table[getSlot(doc)]=doc;
  }

  private int getSlot(int val) {
    int s,v;
    s=val & mask;
    v=table[s];
    // check for EMPTY first since that value is more likely
    if (v==EMPTY || v==val) return s;
    s=rehash(val);
    return s;
  }


  // As the size of this int hashtable is expected to be small
  // (thousands at most), I did not try to keep the rehash function
  // reversible (important to avoid collisions in large hash tables).
  private int rehash(int val) {
    int h,s,v;
    final int comp=~val;

    // don't left shift too far... the only bits
    // that count in the answer are the ones on the right.
    // We want to put more of the bits on the left
    // into the answer.
    // Keep small tables in mind.  We may be only using
    // the first 5 or 6 bits.

    // on the first rehash, use complement instead of val to shift
    // so we don't end up with 0 again if val==0.
    h = val ^ (comp>>8);
    s = h & mask;
    v = table[s];
    if (v==EMPTY || v==val) return s;

    h ^= (v << 17) | (comp >>> 16);   // this is reversible
    s = h & mask;
    v = table[s];
    if (v==EMPTY || v==val) return s;

    h ^= (h << 8) | (comp >>> 25);    // this is reversible
    s = h & mask;
    v = table[s];
    if (v==EMPTY || v==val) return s;

    /**********************
     // Knuth, Thomas Wang, http://www.concentric.net/~Ttwang/tech/inthash.htm
     // This magic number has no common factors with 2^32, and magic/(2^32) approximates
     // the golden ratio.
    private static final int magic = (int)2654435761L;

    h = magic*val;
    s = h & mask;
    v=table[s];
    if (v==EMPTY || v==val) return s;

    // the mult with magic should have thoroughly mixed the bits.
    // add entropy to the right half from the left half.
    h ^= h>>>16;
    s = h & mask;
    v=table[s];
    if (v==EMPTY || v==val) return s;
    *************************/

    // linear scan now... ug.
    final int start=s;
    while (++s<tablesize) {
      v=table[s];
      if (v==EMPTY || v==val) return s;
    }
    s=start;
    while (--s>=0) {
      v=table[s];
      if (v==EMPTY || v==val) return s;
    }
    return s;
  }


  /**
   *
   * @return The number of document ids in the set.
   */
  public int size() {
    return size;
  }

  public boolean exists(int docid) {
    int v = table[docid & mask];
    if (v==EMPTY) return false;
    else if (v==docid) return true;
    else {
      v = table[rehash(docid)];
      if (v==docid) return true;
      else return false;
    }
  }

  public DocIterator iterator() {
    return new DocIterator() {
      int pos=0;
      int doc;
      { goNext(); }

      public boolean hasNext() {
        return pos < tablesize;
      }

      public Integer next() {
        return nextDoc();
      }

      public void remove() {
      }

      void goNext() {
        while (pos<tablesize && table[pos]==EMPTY) pos++;
      }

      // modify to return -1 at end of iteration?
      public int nextDoc() {
        int doc = table[pos];
        pos++;
        goNext();
        return doc;
      }

      public float score() {
        return 0.0f;
      }
    };
  }


  public long memSize() {
    return (tablesize<<2) + 20;
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


  // don't implement andNotSize() and unionSize() on purpose... they are implemented
  // in BaseDocSet in terms of intersectionSize().
}
