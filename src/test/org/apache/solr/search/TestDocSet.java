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

import junit.framework.TestCase;

import java.util.Random;

import org.apache.solr.util.OpenBitSet;
import org.apache.solr.util.BitSetIterator;

/**
 * @version $Id$
 */
public class TestDocSet extends TestCase {
  Random rand = new Random();
  float loadfactor;

  public OpenBitSet getRandomSet(int sz, int bitsToSet) {
    OpenBitSet bs = new OpenBitSet(sz);
    if (sz==0) return bs;
    for (int i=0; i<bitsToSet; i++) {
      bs.fastSet(rand.nextInt(sz));
    }
    return bs;
  }

  public DocSet getHashDocSet(OpenBitSet bs) {
    int[] docs = new int[(int)bs.cardinality()];
    BitSetIterator iter = new BitSetIterator(bs);
    for (int i=0; i<docs.length; i++) {
      docs[i] = iter.next();
    }
    return new HashDocSet(docs,0,docs.length);
  }

  public DocSet getBitDocSet(OpenBitSet bs) {
    return new BitDocSet(bs);
  }

  public DocSet getDocSet(OpenBitSet bs) {
    return rand.nextInt(2)==0 ? getHashDocSet(bs) : getBitDocSet(bs);
  }

  public void checkEqual(OpenBitSet bs, DocSet set) {
    for (int i=0; i<bs.capacity(); i++) {
      assertEquals(bs.get(i), set.exists(i));
    }
  }

  protected void doSingle(int maxSize) {
    int sz = rand.nextInt(maxSize+1);
    int sz2 = rand.nextInt(maxSize);
    OpenBitSet a1 = getRandomSet(sz, rand.nextInt(sz+1));
    OpenBitSet a2 = getRandomSet(sz, rand.nextInt(sz2+1));

    DocSet b1 = getDocSet(a1);
    DocSet b2 = getDocSet(a2);

    // System.out.println("b1="+b1+", b2="+b2);

    assertEquals((int)a1.cardinality(), b1.size());
    assertEquals((int)a2.cardinality(), b2.size());

    checkEqual(a1,b1);
    checkEqual(a2,b2);

    OpenBitSet a_and = (OpenBitSet)a1.clone(); a_and.and(a2);
    OpenBitSet a_or = (OpenBitSet)a1.clone(); a_or.or(a2);
    // OpenBitSet a_xor = (OpenBitSet)a1.clone(); a_xor.xor(a2);
    OpenBitSet a_andn = (OpenBitSet)a1.clone(); a_andn.andNot(a2);

    checkEqual(a_and, b1.intersection(b2));
    checkEqual(a_or, b1.union(b2));
    checkEqual(a_andn, b1.andNot(b2));

    assertEquals(a_and.cardinality(), b1.intersectionSize(b2));
    assertEquals(a_or.cardinality(), b1.unionSize(b2));
    assertEquals(a_andn.cardinality(), b1.andNotSize(b2));
  }


  public void doMany(int maxSz, int iter) {
    for (int i=0; i<iter; i++) {
      doSingle(maxSz);
    }
  }

  public void testRandomDocSets() {
    doMany(300, 5000);
  }


  public HashDocSet getRandomHashDocset(int maxSetSize, int maxDoc) {
    int n = rand.nextInt(maxSetSize);
    OpenBitSet obs = new OpenBitSet(maxDoc);
    int[] a = new int[n];
    for (int i=0; i<n; i++) {
      for(;;) {
        int idx = rand.nextInt(maxDoc);
        if (obs.getAndSet(idx)) continue;
        a[i]=idx;
        break;
      }
    }
    return loadfactor!=0 ? new HashDocSet(a,0,n,1/loadfactor) : new HashDocSet(a,0,n);
  }

  public DocSet[] getRandomHashSets(int nSets, int maxSetSize, int maxDoc) {
    DocSet[] sets = new DocSet[nSets];

    for (int i=0; i<nSets; i++) {
      sets[i] = getRandomHashDocset(maxSetSize,maxDoc);
    }

    return sets;
  }

  /**** needs code insertion into HashDocSet
  public void testCollisions() {
    loadfactor=.75f;
    rand=new Random(12345);  // make deterministic
    int maxSetsize=4000;
    int nSets=256;
    int iter=1;
    int[] maxDocs=new int[] {100000,500000,1000000,5000000,10000000};
    int ret=0;
    long start=System.currentTimeMillis();
    for (int maxDoc : maxDocs) {
      int cstart = HashDocSet.collisions;
      DocSet[] sets = getRandomHashSets(nSets,maxSetsize, maxDoc);
      for (DocSet s1 : sets) {
        for (DocSet s2 : sets) {
          if (s1!=s2) ret += s1.intersectionSize(s2);
        }
      }
      int cend = HashDocSet.collisions;
      System.out.println("maxDoc="+maxDoc+"\tcollisions="+(cend-cstart));      
    }
    long end=System.currentTimeMillis();
    System.out.println("testIntersectionSizePerformance="+(end-start)+" ms");
    if (ret==-1)System.out.println("wow!");
    System.out.println("collisions="+HashDocSet.collisions);

  }
  ***/

  /***
  public void testIntersectionSizePerformance() {
    loadfactor=.75f;
    rand=new Random(12345);  // make deterministic
    int maxSetsize=4000;
    int nSets=128;
    int iter=10;
    int maxDoc=1000000;
    DocSet[] sets = getRandomHashSets(nSets,maxSetsize, maxDoc);
    int ret=0;
    long start=System.currentTimeMillis();
    for (int i=0; i<iter; i++) {
      for (DocSet s1 : sets) {
        for (DocSet s2 : sets) {
          ret += s1.intersectionSize(s2);
        }
      }
    }
    long end=System.currentTimeMillis();
    System.out.println("testIntersectionSizePerformance="+(end-start)+" ms");
    if (ret==-1)System.out.println("wow!");
  }


  public void testExistsPerformance() {
    loadfactor=.75f;
    rand=new Random(12345);  // make deterministic
    int maxSetsize=4000;
    int nSets=512;
    int iter=1;
    int maxDoc=1000000;
    DocSet[] sets = getRandomHashSets(nSets,maxSetsize, maxDoc);
    int ret=0;
    long start=System.currentTimeMillis();
    for (int i=0; i<iter; i++) {
      for (DocSet s1 : sets) {
        for (int j=0; j<maxDoc; j++) {
          ret += s1.exists(j) ? 1 :0;
        }
      }
    }
    long end=System.currentTimeMillis();
    System.out.println("testExistsSizePerformance="+(end-start)+" ms");
    if (ret==-1)System.out.println("wow!");
  }
   ***/

   /**** needs code insertion into HashDocSet
   public void testExistsCollisions() {
    loadfactor=.75f;
    rand=new Random(12345);  // make deterministic
    int maxSetsize=4000;
    int nSets=512;
    int[] maxDocs=new int[] {100000,500000,1000000,5000000,10000000};
    int ret=0;

    for (int maxDoc : maxDocs) {
      int mask = (BitUtil.nextHighestPowerOfTwo(maxDoc)>>1)-1;
      DocSet[] sets = getRandomHashSets(nSets,maxSetsize, maxDoc);
      int cstart = HashDocSet.collisions;      
      for (DocSet s1 : sets) {
        for (int j=0; j<maxDocs[0]; j++) {
          int idx = rand.nextInt()&mask;
          ret += s1.exists(idx) ? 1 :0;
        }
      }
      int cend = HashDocSet.collisions;
      System.out.println("maxDoc="+maxDoc+"\tcollisions="+(cend-cstart));
    }
    if (ret==-1)System.out.println("wow!");
    System.out.println("collisions="+HashDocSet.collisions);
  }
  ***/


}
