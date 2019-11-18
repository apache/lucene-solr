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
package org.apache.solr.handler.admin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.api.collections.SplitByPrefixTest;
import org.apache.solr.cloud.api.collections.SplitByPrefixTest.Prefix;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

// test low level splitByPrefix range recommendations.
// This is here to access package private methods.
// See SplitByPrefixTest for cloud level tests of SPLITSHARD that use this by passing getRanges with the SPLIT command
public class SplitHandlerTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("managed.schema.mutable", "true");  // needed by cloud-managed config set
    initCore("solrconfig.xml","schema_latest.xml");
  }

  void verifyContiguous(Collection<DocRouter.Range> results, DocRouter.Range currentRange) {
    if (results == null) return;

    assertTrue(results.size() > 1);

    DocRouter.Range prev = null;
    for (DocRouter.Range range : results) {
      if (prev == null) {
        // first range
        assertEquals(range.min, currentRange.min);
      } else {
        // make sure produced ranges are contiguous
        assertEquals(range.min, prev.max + 1);
      }
      prev = range;
    }
    assertEquals(prev.max, currentRange.max);
  }


  // bias around special numbers
  int randomBound(Random rand) {
    int ret = 0;
    switch(rand.nextInt(10)) {
      case 0: ret = Integer.MIN_VALUE; break;
      case 1: ret = Integer.MAX_VALUE; break;
      case 2: ret = 0; break;
      default: ret = rand.nextInt();
    }
    if (rand.nextBoolean()) {
      ret += rand.nextInt(2000) - 1000;
    }
    return ret;
  }

  @Test
  public void testRandomSplitRecommendations() throws Exception {
    Random rand = random();
    for (int i=0; i<10000; i++) { // 1M takes ~ 1 sec
      doRandomSplitRecommendation(rand);
    }
  }

  public void doRandomSplitRecommendation(Random rand) throws Exception {
    int low = 0;
    int high = 0;

    while (high-low < 10) {
      low = randomBound(rand);
      high = randomBound(rand);
      if (low > high) {
        int tmp = low;
        low = high;
        high = tmp;
      }
    }

    DocRouter.Range curr = new DocRouter.Range(low,high);


    int maxRanges = rand.nextInt(20);

    int start = low;

    // bucket can start before or after
    if (rand.nextBoolean()) {
        start += rand.nextInt(200) - 100;
        if (start > low) {
          // underflow
          start = Integer.MIN_VALUE;
        }
    }

    List<SplitOp.RangeCount> counts = new ArrayList<>(maxRanges);
    for (;;) {
      int end = start + rand.nextInt(100) + 1;
      if (end < start) {
        // overflow
        end = Integer.MAX_VALUE;
      }
      counts.add( new SplitOp.RangeCount(new DocRouter.Range(start, end), rand.nextInt(1000)+1));
      if (counts.size() >= maxRanges) break;
      if (counts.size() == maxRanges / 2 && rand.nextBoolean()) {
        // transition toward the end of the range (more boundary cases for large ranges)
        start = high - rand.nextInt(100);
        start = Math.max(start, end+1);
      } else {
        start = end + 1;
      }
      if (rand.nextBoolean()) {
        start += rand.nextInt(100);
      }
      if (start < end) {
        // overflow
        break;
      }
    }

    try {
      Collection<DocRouter.Range> results = SplitOp.getSplits(counts, curr);
      verifyContiguous(results, curr);
    } catch (Throwable e) {
      // System.err.println(e);
    }
  }


  @Test
  public void testSplitRecommendations() throws Exception {

    // split whole range exactly in two
    DocRouter.Range curr = new DocRouter.Range(10,15);
    List<SplitOp.RangeCount> counts = new ArrayList<>();
    counts.add(new SplitOp.RangeCount(new DocRouter.Range(10,15), 100));
    Collection<DocRouter.Range> results = SplitOp.getSplits(counts, curr);
    assertEquals(12, results.iterator().next().max);
    verifyContiguous(results, curr);

    // make sure range with docs is split in half even if current range of shard is bigger
    curr = new DocRouter.Range(-100,101);
    counts = new ArrayList<>();
    counts.add(new SplitOp.RangeCount(new DocRouter.Range(10,15), 100));
    results = SplitOp.getSplits(counts, curr);
    assertEquals(12, results.iterator().next().max);
    verifyContiguous(results, curr);

    // don't freak out if we encounter some ranges outside of the current defined shard range
    // this can happen since document routing can be overridden.
    curr = new DocRouter.Range(-100,101);
    counts = new ArrayList<>();
    counts.add(new SplitOp.RangeCount(new DocRouter.Range(-1000,-990), 100));
    counts.add(new SplitOp.RangeCount(new DocRouter.Range(-980,-970), 2));
    counts.add(new SplitOp.RangeCount(new DocRouter.Range(10,15), 100));
    counts.add(new SplitOp.RangeCount(new DocRouter.Range(1000,1010), 5));
    counts.add(new SplitOp.RangeCount(new DocRouter.Range(1020,1030), 7));
    results = SplitOp.getSplits(counts, curr);
    assertEquals(12, results.iterator().next().max);
    verifyContiguous(results, curr);


    // splitting counts of [1,4,3] should result in [1,4],[3]
    // splitting count sof [3,4,1] should result in [3],[4,1]
    // The current implementation has specific code for the latter case (hence this is needed for code coverage)
    // The random tests *should* catch this as well though.
    curr = new DocRouter.Range(-100,101);
    counts = new ArrayList<>();
    counts.add(new SplitOp.RangeCount(new DocRouter.Range(0,9), 1));
    counts.add(new SplitOp.RangeCount(new DocRouter.Range(10,19), 4));
    counts.add(new SplitOp.RangeCount(new DocRouter.Range(20,29), 3));
    results = SplitOp.getSplits(counts, curr);
    assertEquals(19, results.iterator().next().max);
    verifyContiguous(results, curr);

    curr = new DocRouter.Range(-100,101);
    counts = new ArrayList<>();
    counts.add(new SplitOp.RangeCount(new DocRouter.Range(0,9), 3));
    counts.add(new SplitOp.RangeCount(new DocRouter.Range(10,19), 4));
    counts.add(new SplitOp.RangeCount(new DocRouter.Range(20,29), 1));
    results = SplitOp.getSplits(counts, curr);
    assertEquals(9, results.iterator().next().max);
    verifyContiguous(results, curr);


    // test that if largest count is first
    curr = new DocRouter.Range(-100,101);
    counts = new ArrayList<>();
    counts.add(new SplitOp.RangeCount(new DocRouter.Range(0,9), 4));
    counts.add(new SplitOp.RangeCount(new DocRouter.Range(10,19), 1));
    counts.add(new SplitOp.RangeCount(new DocRouter.Range(20,29), 1));
    results = SplitOp.getSplits(counts, curr);
    assertEquals(9, results.iterator().next().max);
    verifyContiguous(results, curr);

    // test that if largest count is last (this has specific code since we don't get over midpoint until the last range and then need to back up)
    curr = new DocRouter.Range(-100,101);
    counts = new ArrayList<>();
    counts.add(new SplitOp.RangeCount(new DocRouter.Range(0,9), 1));
    counts.add(new SplitOp.RangeCount(new DocRouter.Range(10,19), 1));
    counts.add(new SplitOp.RangeCount(new DocRouter.Range(20,29), 4));
    results = SplitOp.getSplits(counts, curr);
    assertEquals(19, results.iterator().next().max);
    verifyContiguous(results, curr);
  }

  @Test
  public void testHistogramBuilding() throws Exception {
    List<Prefix> prefixes = SplitByPrefixTest.findPrefixes(20, 0, 0x00ffffff);
    List<Prefix> uniquePrefixes = SplitByPrefixTest.removeDups(prefixes);
    assertTrue(prefixes.size() > uniquePrefixes.size());  // make sure we have some duplicates to test hash collisions

    String prefixField = "id_prefix_s";
    String idField = "id";
    DocRouter router = new CompositeIdRouter();


    for (int i=0; i<100; i++) {
      SolrQueryRequest req = req("myquery");
      try {
        // the first time through the loop we do this before adding docs to test an empty index
        Collection<SplitOp.RangeCount> counts1 = SplitOp.getHashHistogram(req.getSearcher(), prefixField, router, null);
        Collection<SplitOp.RangeCount> counts2 = SplitOp.getHashHistogramFromId(req.getSearcher(), idField, router, null);
        assertTrue(eqCount(counts1, counts2));

        if (i>0) {
          assertTrue(counts1.size() > 0);  // make sure we are testing something
        }


        // index a few random documents
        int ndocs = random().nextInt(10) + 1;
        for (int j=0; j<ndocs; j++) {
          String prefix = prefixes.get( random().nextInt(prefixes.size()) ).key;
          if (random().nextBoolean()) {
            prefix = prefix + Integer.toString(random().nextInt(3)) + "!";
          }
          String id = prefix + "doc" + i + "_" + j;
          updateJ(jsonAdd(sdoc(idField, id, prefixField, prefix)), null);
        }

        assertU(commit());


      } finally {
        req.close();
      }

    }

  }

  private boolean eqCount(Collection<SplitOp.RangeCount> a, Collection<SplitOp.RangeCount> b) {
    if (a.size() != b.size()) {
      return false;
    }
    
    Iterator<SplitOp.RangeCount> it1 = a.iterator();
    Iterator<SplitOp.RangeCount> it2 = b.iterator();
    while (it1.hasNext()) {
      SplitOp.RangeCount r1 = it1.next();
      SplitOp.RangeCount r2 = it2.next();
      if (!r1.range.equals(r2.range) || r1.count != r2.count) {
        return false;
      }
    }
    return true;
  }

}
