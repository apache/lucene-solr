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
package org.apache.solr.cloud;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.DocRouter.Range;
import org.apache.solr.common.cloud.PlainIdRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.Hash;
import org.apache.solr.common.util.StrUtils;

public class TestHashPartitioner extends SolrTestCaseJ4 {
  
  public void testMapHashes() throws Exception {
    DocRouter hp = DocRouter.DEFAULT;
    List<Range> ranges;

    // make sure the partitioner uses the "natural" boundaries and doesn't suffer from an off-by-one
    ranges = hp.partitionRange(2, hp.fullRange());
    assertEquals(Integer.MIN_VALUE, ranges.get(0).min);
    assertEquals(0x80000000, ranges.get(0).min);
    assertEquals(0xffffffff, ranges.get(0).max);
    assertEquals(0x00000000, ranges.get(1).min);
    assertEquals(0x7fffffff, ranges.get(1).max);

    ranges = hp.partitionRange(2, new DocRouter.Range(0, 0x7fffffff));
    assertEquals(0x00000000, ranges.get(0).min);
    assertEquals(0x3fffffff, ranges.get(0).max);
    assertEquals(0x40000000, ranges.get(1).min);
    assertEquals(0x7fffffff, ranges.get(1).max);

    int defaultLowerBits = 0x0000ffff;

    for (int i = 1; i <= 30000; i++) {
      // start skipping at higher numbers
      if (i > 100) i+=13;
      else if (i > 1000) i+=31;
      else if (i > 5000) i+=101;

      long rangeSize = 0x0000000100000000L / i;

      ranges = hp.partitionRange(i, hp.fullRange());
      assertEquals(i, ranges.size());
      assertTrue("First range does not start before " + Integer.MIN_VALUE
          + " it is:" + ranges.get(0).min,
          ranges.get(0).min <= Integer.MIN_VALUE);
      assertTrue("Last range does not end after " + Integer.MAX_VALUE
          + " it is:" + ranges.get(ranges.size() - 1).max,
          ranges.get(ranges.size() - 1).max >= Integer.MAX_VALUE);

      for (Range range : ranges) {
        String s = range.toString();
        Range newRange = hp.fromString(s);
        assertEquals(range, newRange);
      }

      // ensure that ranges are contiguous and that size deviations are not too large.
      int lastEnd = Integer.MIN_VALUE - 1;
      for (Range range : ranges) {
        int currStart = range.min;
        int currEnd = range.max;
        assertEquals(lastEnd+1, currStart);

        if (ranges.size() < 4000) {
          // ranges should be rounded to avoid crossing hash domains
          assertEquals(defaultLowerBits, currEnd & defaultLowerBits);

          // given our rounding condition that domains should be less than 1/16 of the step size,
          // this means that any sizing deviations should also be less than 1/16th of the idealized range size.
          // boolean round = rangeStep >= (1<<bits)*16;

          long currRangeSize = (long)currEnd - (long)currStart;
          long error = Math.abs(rangeSize - currRangeSize);
          assertTrue( error < rangeSize/16);
        }


        // String s = range.toString();
        // Range newRange = hp.fromString(s);
        // assertEquals(range, newRange);
        lastEnd = currEnd;
      }

    }
  }

  public int hash(String id) {
    // our hashing is defined to be murmurhash3 on the UTF-8 bytes of the key.
    return Hash.murmurhash3_x86_32(id, 0, id.length(), 0);
  }

  public void testHashCodes() throws Exception {
    DocRouter router = DocRouter.getDocRouter(PlainIdRouter.NAME);
    assertTrue(router instanceof PlainIdRouter);
    DocCollection coll = createCollection(4, router);
    doNormalIdHashing(coll);
  }

  public void doNormalIdHashing(DocCollection coll) throws Exception {
    assertEquals(4, coll.getSlices().size());

    doId(coll, "b", "shard1");
    doId(coll, "c", "shard2");
    doId(coll, "d", "shard3");
    doId(coll, "e", "shard4");
  }

  public void doId(DocCollection coll, String id, String expectedShard) {
    doIndex(coll, id, expectedShard);
    doQuery(coll, id, expectedShard);
  }

  public void doIndex(DocCollection coll, String id, String expectedShard) {
    DocRouter router = coll.getRouter();
    Slice target = router.getTargetSlice(id, null, null, null, coll);
    assertEquals(expectedShard, target.getName());
  }

  public void doQuery(DocCollection coll, String id, String expectedShards) {
    DocRouter router = coll.getRouter();
    Collection<Slice> slices = router.getSearchSlices(id, null, coll);

    List<String> expectedShardStr = StrUtils.splitSmart(expectedShards, ",", true);

    HashSet<String> expectedSet = new HashSet<>(expectedShardStr);
    HashSet<String> obtainedSet = new HashSet<>();
    for (Slice slice : slices) {
      obtainedSet.add(slice.getName());
    }

    assertEquals(slices.size(), obtainedSet.size());  // make sure no repeated slices
    assertEquals(expectedSet, obtainedSet);
  }

  public void testCompositeHashCodes() throws Exception {
    DocRouter router = DocRouter.getDocRouter(CompositeIdRouter.NAME);
    assertTrue(router instanceof CompositeIdRouter);
    router = DocRouter.DEFAULT;
    assertTrue(router instanceof CompositeIdRouter);

    DocCollection coll = createCollection(4, router);
    doNormalIdHashing(coll);

    // ensure that the shard hashed to is only dependent on the first part of the compound key
    doId(coll, "b!foo", "shard1");
    doId(coll, "c!bar", "shard2");
    doId(coll, "d!baz", "shard3");
    doId(coll, "e!qux", "shard4");

    // syntax to specify bits.
    // Anything over 2 bits should give the same results as above (since only top 2 bits
    // affect our 4 slice collection).
    doId(coll, "b/2!foo", "shard1");
    doId(coll, "c/2!bar", "shard2");
    doId(coll, "d/2!baz", "shard3");
    doId(coll, "e/2!qux", "shard4");

    doId(coll, "b/32!foo", "shard1");
    doId(coll, "c/32!bar", "shard2");
    doId(coll, "d/32!baz", "shard3");
    doId(coll, "e/32!qux", "shard4");

    // no bits allocated to the first part (kind of odd why anyone would do that though)
    doIndex(coll, "foo/0!b", "shard1");
    doIndex(coll, "foo/0!c", "shard2");
    doIndex(coll, "foo/0!d", "shard3");
    doIndex(coll, "foo/0!e", "shard4");

    // means cover whole range on the query side
    doQuery(coll, "foo/0!", "shard1,shard2,shard3,shard4");

    doQuery(coll, "b/1!", "shard1,shard2");   // top bit of hash(b)==1, so shard1 and shard2
    doQuery(coll, "d/1!", "shard3,shard4");   // top bit of hash(b)==0, so shard3 and shard4
  }

  /** Make sure CompositeIdRouter doesn't throw exceptions for non-conforming IDs */
  public void testNonConformingCompositeIds() throws Exception {
    DocRouter router = DocRouter.getDocRouter(CompositeIdRouter.NAME);
    DocCollection coll = createCollection(4, router);
    String[] ids = { "A!B!C!D", "!!!!!!", "A!!!!B", "A!!B!!C", "A/59!B", "A/8/!B/19/", 
                     "A!B/-5", "!/130!", "!!A/1000", "A//8!B///10!C////" };
    for (int i = 0 ; i < ids.length ; ++i) {
      try {
        Slice targetSlice = coll.getRouter().getTargetSlice(ids[i], null, null, null, coll);
        assertNotNull(targetSlice);
      } catch (Exception e) {
        throw new Exception("Exception routing id '" + ids[i] + "'", e);
      }
    }
  }

  /** Make sure CompositeIdRouter can route random IDs without throwing exceptions */
  public void testRandomCompositeIds() throws Exception {
    DocRouter router = DocRouter.getDocRouter(CompositeIdRouter.NAME);
    DocCollection coll = createCollection(TestUtil.nextInt(random(), 1, 10), router);
    StringBuilder idBuilder = new StringBuilder();
    for (int i = 0 ; i < 10000 ; ++i) {
      idBuilder.setLength(0);
      int numParts = TestUtil.nextInt(random(), 1, 30);
      for (int part = 0; part < numParts; ++part) {
        switch (random().nextInt(5)) {
          case 0: idBuilder.append('!'); break;
          case 1: idBuilder.append('/'); break;
          case 2: idBuilder.append(TestUtil.nextInt(random(),-100, 1000)); break;
          default: {
            int length = TestUtil.nextInt(random(), 1, 10);
            char[] str = new char[length];
            TestUtil.randomFixedLengthUnicodeString(random(), str, 0, length);
            idBuilder.append(str);
            break;
          } 
        }
      }
      String id = idBuilder.toString();
      try {
        Slice targetSlice = router.getTargetSlice(id, null, null, null, coll);
        assertNotNull(targetSlice);
      } catch (Exception e) {
        throw new Exception("Exception routing id '" + id + "'", e);
      }
    }
  }

  /***
    public void testPrintHashCodes() throws Exception {
     // from negative to positive, the upper bits of the hash ranges should be
     // shard1: 11
     // shard2: 10
     // shard3: 00
     // shard4: 01
  
     String[] highBitsToShard = {"shard3","shard4","shard1","shard2"};
  
  
     for (int i = 0; i<26; i++) {
        String id  = new String(Character.toChars('a'+i));
        int hash = hash(id);
        System.out.println("hash of " + id + " is " + Integer.toHexString(hash) + " high bits=" + (hash>>>30)
            + " shard="+highBitsToShard[hash>>>30]);
      }
    }
    ***/



  @SuppressWarnings({"unchecked"})
  DocCollection createCollection(int nSlices, DocRouter router) {
    List<Range> ranges = router.partitionRange(nSlices, router.fullRange());

    Map<String,Slice> slices = new HashMap<>();
    for (int i=0; i<ranges.size(); i++) {
      Range range = ranges.get(i);
      Slice slice = new Slice("shard"+(i+1), null, map("range",range), "collections1");
      slices.put(slice.getName(), slice);
    }

    DocCollection coll = new DocCollection("collection1", slices, null, router);
    return coll;
  }



  // from negative to positive, the upper bits of the hash ranges should be
  // shard1: top bits:10  80000000:bfffffff
  // shard2: top bits:11  c0000000:ffffffff
  // shard3: top bits:00  00000000:3fffffff
  // shard4: top bits:01  40000000:7fffffff

  /***
   hash of a is 3c2569b2 high bits=0 shard=shard3
   hash of b is 95de7e03 high bits=2 shard=shard1
   hash of c is e132d65f high bits=3 shard=shard2
   hash of d is 27191473 high bits=0 shard=shard3
   hash of e is 656c4367 high bits=1 shard=shard4
   hash of f is 2b64883b high bits=0 shard=shard3
   hash of g is f18ae416 high bits=3 shard=shard2
   hash of h is d482b2d3 high bits=3 shard=shard2
   hash of i is 811a702b high bits=2 shard=shard1
   hash of j is ca745a39 high bits=3 shard=shard2
   hash of k is cfbda5d1 high bits=3 shard=shard2
   hash of l is 1d5d6a2c high bits=0 shard=shard3
   hash of m is 5ae4385c high bits=1 shard=shard4
   hash of n is c651d8ac high bits=3 shard=shard2
   hash of o is 68348473 high bits=1 shard=shard4
   hash of p is 986fdf9a high bits=2 shard=shard1
   hash of q is ff8209e8 high bits=3 shard=shard2
   hash of r is 5c9373f1 high bits=1 shard=shard4
   hash of s is ff4acaf1 high bits=3 shard=shard2
   hash of t is ca87df4d high bits=3 shard=shard2
   hash of u is 62203ae0 high bits=1 shard=shard4
   hash of v is bdafcc55 high bits=2 shard=shard1
   hash of w is ff439d1f high bits=3 shard=shard2
   hash of x is 3e9a9b1b high bits=0 shard=shard3
   hash of y is 477d9216 high bits=1 shard=shard4
   hash of z is c1f69a17 high bits=3 shard=shard2
   ***/

}
