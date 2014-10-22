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

package org.apache.solr.util;

import java.util.BitSet;
import java.util.Random;

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.FixedBitSet.FixedBitSetIterator;

/** Performance tester for FixedBitSet.
 * Use -Xbatch for more predictable results, and run tests such that the duration
 * is at least 10 seconds for better accuracy.  Close browsers on your system (javascript
 * or flash may be running and cause more erratic results).
 *
 *
 */
public class BitSetPerf {
  static Random rand = new Random(0);

  static void randomSets(int maxSize, int bitsToSet, BitSet target1, FixedBitSet target2) {
    for (int i=0; i<bitsToSet; i++) {
      int idx;
      do {
        idx = rand.nextInt(maxSize);
      } while (target2.getAndSet(idx));
      target1.set(idx);
    }
    /***
     int i=target1.cardinality();
     if (i!=bitsToSet || i!=target2.cardinality()) throw new RuntimeException();
     ***/
  }

  public static void main(String[] args) {
    if (args.length<5) {
      System.out.println("BitSetTest <bitSetSize> <numSets> <numBitsSet> <testName> <iter> <impl>");
      System.out.println("  impl => open for FixedBitSet");
    }
    int bitSetSize = Integer.parseInt(args[0]);
    int numSets = Integer.parseInt(args[1]);
    int numBitsSet = Integer.parseInt(args[2]);
    String test = args[3];
    int iter = Integer.parseInt(args[4]);
    String impl = args.length>5 ? args[5].intern() : "bit";

    BitSet[] sets = new BitSet[numSets];
    FixedBitSet[] osets = new FixedBitSet[numSets];

    for (int i=0; i<numSets; i++) {
      sets[i] = new BitSet(bitSetSize);
      osets[i] = new FixedBitSet(bitSetSize);
      randomSets(bitSetSize, numBitsSet, sets[i], osets[i]);
    }

    BitSet bs = new BitSet(bitSetSize);
    FixedBitSet obs = new FixedBitSet(bitSetSize);
    randomSets(bitSetSize, numBitsSet, bs, obs);



    int ret=0;

    long start = System.currentTimeMillis();

    if ("union".equals(test)) {
      for (int it=0; it<iter; it++) {
        for (int i=0; i<numSets; i++) {
          if (impl=="open") {
            FixedBitSet other=osets[i];
            obs.or(other);
          } else {
            BitSet other=sets[i];
            bs.or(other);
          }
        }
      }
    }

    if ("cardinality".equals(test)) {
      for (int it=0; it<iter; it++) {
        for (int i=0; i<numSets; i++) {
          if (impl=="open") {
            ret += osets[i].cardinality();
          } else {
            ret += sets[i].cardinality();
          }
        }
      }
    }

    if ("get".equals(test)) {
      for (int it=0; it<iter; it++) {
        for (int i=0; i<numSets; i++) {
          if (impl=="open") {
            FixedBitSet oset = osets[i];
            for (int k=0; k<bitSetSize; k++) if (oset.get(k)) ret++;
          } else {
            BitSet bset = sets[i];
            for (int k=0; k<bitSetSize; k++) if (bset.get(k)) ret++;
          }
        }
      }
    }

    if ("icount".equals(test)) {
      for (int it=0; it<iter; it++) {
        for (int i=0; i<numSets-1; i++) {
          if (impl=="open") {
            FixedBitSet a=osets[i];
            FixedBitSet b=osets[i+1];
            ret += FixedBitSet.intersectionCount(a,b);
          } else {
            BitSet a=sets[i];
            BitSet b=sets[i+1];
            BitSet newset = (BitSet)a.clone();
            newset.and(b);
            ret += newset.cardinality();
          }
        }
      }
    }

    if ("clone".equals(test)) {
      for (int it=0; it<iter; it++) {
        for (int i=0; i<numSets; i++) {
          if (impl=="open") {
            osets[i] = osets[i].clone();
          } else {
            sets[i] = (BitSet)sets[i].clone();
          }
        }
      }
    }

    if ("nextSetBit".equals(test)) {
      for (int it=0; it<iter; it++) {
        for (int i=0; i<numSets; i++) {
          if (impl=="open") {
            final FixedBitSet set = osets[i];
            for(int next=set.nextSetBit(0); next>=0; next=set.nextSetBit(next+1)) {
              ret += next;
            }
          } else {
            final BitSet set = sets[i];
            for(int next=set.nextSetBit(0); next>=0; next=set.nextSetBit(next+1)) {
              ret += next;
            }
          }
        }
      }
    }


    if ("iterator".equals(test)) {
      for (int it=0; it<iter; it++) {
        for (int i=0; i<numSets; i++) {
          if (impl=="open") {
            final FixedBitSet set = osets[i];
            final FixedBitSetIterator iterator = new FixedBitSetIterator(set, 0);
            for(int next=iterator.nextDoc(); next>=0; next=iterator.nextDoc()) {
              ret += next;
            }
          } else {
            final BitSet set = sets[i];
            for(int next=set.nextSetBit(0); next>=0; next=set.nextSetBit(next+1)) {
              ret += next;
            }
          }
        }
      }
    }

    long end = System.currentTimeMillis();
    System.out.println("ret="+ret);
    System.out.println("TIME="+(end-start));

  }


}
