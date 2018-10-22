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

import java.util.Random;

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.solr.util.RTimer;

/**
 */
public class DocSetPerf {

  // use test instead of assert since asserts may be turned off
  public static void test(boolean condition) {
      if (!condition) {
        throw new RuntimeException("test requestHandler: assertion failed!");
      }
  }

  static FixedBitSet bs;
  static BitDocSet bds;
  static HashDocSet hds;
  static int[] ids; // not unique

  static Random rand = getRandom();
  
  @SuppressForbidden(reason = "No testcase, use of java.util.Random allowed")
  private static Random getRandom() {
    return new Random();
  }

  static void generate(int maxSize, int bitsToSet) {
    bs = new FixedBitSet(maxSize);
    ids = new int[bitsToSet];
    int count=0;
    if (maxSize>0) {
      for (int i=0; i<bitsToSet; i++) {
        int id=rand.nextInt(maxSize);
        if (!bs.get(id)) {
          bs.set(id);
          ids[count++]=id;
        }
      }
    }
    bds = new BitDocSet(bs,bitsToSet);
    hds = new HashDocSet(ids,0,count);
  }

  public static void main(String[] args) {
    String bsSize=args[0];
    boolean randSize=false;

    if (bsSize.endsWith("-")) {
      bsSize=bsSize.substring(0,bsSize.length()-1);
      randSize=true;
    }

    int bitSetSize = Integer.parseInt(bsSize);
    int numSets = Integer.parseInt(args[1]);
    int numBitsSet = Integer.parseInt(args[2]);
    String test = args[3].intern();
    int iter = Integer.parseInt(args[4]);

    long ret=0;

    FixedBitSet[] sets = new FixedBitSet[numSets];
    DocSet[] bset = new DocSet[numSets];
    DocSet[] hset = new DocSet[numSets];
    
    for (int i=0; i<numSets; i++) {
      generate(randSize ? rand.nextInt(bitSetSize) : bitSetSize, numBitsSet);
      sets[i] = bs;
      bset[i] = bds;
      hset[i] = hds;
    }

    final RTimer timer = new RTimer();

    if ("test".equals(test)) {
      for (int it=0; it<iter; it++) {
        generate(randSize ? rand.nextInt(bitSetSize) : bitSetSize, numBitsSet);
        FixedBitSet bs1=bs;
        BitDocSet bds1=bds;
        HashDocSet hds1=hds;
        generate(randSize ? rand.nextInt(bitSetSize) : bitSetSize, numBitsSet);

        FixedBitSet res = bs1.clone();
        res.and(bs);
        int icount = res.cardinality();

        test(bds1.intersection(bds).size() == icount);
        test(bds1.intersectionSize(bds) == icount);
        if (bds1.intersection(hds).size() != icount) {
          DocSet ds = bds1.intersection(hds);
          System.out.println("STOP");
        }

        test(bds1.intersection(hds).size() == icount);
        test(bds1.intersectionSize(hds) == icount);
        test(hds1.intersection(bds).size() == icount);
        test(hds1.intersectionSize(bds) == icount);
        test(hds1.intersection(hds).size() == icount);
        test(hds1.intersectionSize(hds) == icount);

        ret += icount;
      }
    }

    String type=null;
    String oper=null;

    if (test.endsWith("B")) { type="B"; }
    if (test.endsWith("H")) { type="H"; }
    if (test.endsWith("M")) { type="M"; }
    if (test.startsWith("intersect")) oper="intersect";
    if (test.startsWith("intersectSize")) oper="intersectSize";
    if (test.startsWith("intersectAndSize")) oper="intersectSize";


    if (oper!=null) {
      for (int it=0; it<iter; it++) {
        int idx1 = rand.nextInt(numSets);
        int idx2 = rand.nextInt(numSets);
        DocSet a=null,b=null;

        if (type=="B") {
          a=bset[idx1]; b=bset[idx2];
        } else if (type=="H") {
          a=hset[idx1]; b=bset[idx2];
        } else if (type=="M") {
          if (idx1 < idx2) {
            a=bset[idx1];
            b=hset[idx2];
          } else {
            a=hset[idx1];
            b=bset[idx2];
          }
        }

        if (oper=="intersect") {
          DocSet res = a.intersection(b);
          ret += res.ramBytesUsed();
        } else if (oper=="intersectSize") {
          ret += a.intersectionSize(b);
        } else if (oper=="intersectAndSize") {
          DocSet res = a.intersection(b);
          ret += res.size();
        }
      }
    }

    System.out.println("TIME="+timer.getTime());

    System.out.println("ret="+ret);
  }

}
