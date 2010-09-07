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

package org.apache.solr.spelling.suggest;

import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.SpellingParams;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.spelling.suggest.Lookup.LookupResult;
import org.apache.solr.spelling.suggest.jaspell.JaspellLookup;
import org.apache.solr.spelling.suggest.tst.TSTLookup;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.TermFreqIterator;
import org.apache.solr.util.TestHarness;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class SuggesterTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-spellchecker.xml","schema-spellchecker.xml");
  }

  public static void addDocs() throws Exception {
    assertU(adoc("id", "1",
                 "text", "acceptable accidentally accommodate acquire"
               ));
    assertU(adoc("id", "2",
                 "text", "believe bellwether accommodate acquire"
               ));
    assertU(adoc("id", "3",
                "text", "cemetery changeable conscientious consensus acquire bellwether"
               ));
  }
  
  @Test
  public void testSuggestions() throws Exception {
    addDocs();

    assertU(commit()); // configured to do a rebuild on commit

    assertQ(req("qt","/suggest", "q","ac", SpellingParams.SPELLCHECK_COUNT, "2", SpellingParams.SPELLCHECK_ONLY_MORE_POPULAR, "true"),
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/int[@name='numFound'][.='2']",
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/arr[@name='suggestion']/str[1][.='acquire']",
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/arr[@name='suggestion']/str[2][.='accommodate']"
    );
  }

  
  private TermFreqIterator getTFIT() {
    final int count = 100000;
    TermFreqIterator tfit = new TermFreqIterator() {
      Random r = new Random(1234567890L);
      Random r1 = new Random(1234567890L);
      int pos;

      public float freq() {
        return r1.nextInt(4);
      }

      public boolean hasNext() {
        return pos < count;
      }

      public String next() {
        pos++;
        return Long.toString(r.nextLong());
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
      
    };
    return tfit;
  }
  
  private void _benchmark(Lookup lookup, Map<String,Integer> ref, boolean estimate, Bench bench) throws Exception {
    long start = System.currentTimeMillis();
    lookup.build(getTFIT());
    long buildTime = System.currentTimeMillis() - start;
    TermFreqIterator tfit = getTFIT();
    long elapsed = 0;
    while (tfit.hasNext()) {
      String key = tfit.next();
      // take only the first part of the key
      int len = key.length() > 4 ? key.length() / 3 : 2;
      String prefix = key.substring(0, len);
      start = System.nanoTime();
      List<LookupResult> res = lookup.lookup(prefix, true, 10);
      elapsed += System.nanoTime() - start;
      assertTrue(res.size() > 0);
      for (LookupResult lr : res) {
        assertTrue(lr.key.startsWith(prefix));
      }
      if (ref != null) { // verify the counts
        Integer Cnt = ref.get(key);
        if (Cnt == null) { // first pass
          ref.put(key, res.size());
        } else {
          assertEquals(key + ", prefix: " + prefix, Cnt.intValue(), res.size());
        }
      }
    }
    if (estimate) {
      RamUsageEstimator rue = new RamUsageEstimator();
      long size = rue.estimateRamUsage(lookup);
      System.err.println(lookup.getClass().getSimpleName() + " - size=" + size);
    }
    if (bench != null) {
      bench.buildTime += buildTime;
      bench.lookupTime +=  elapsed;
    }
  }
  
  class Bench {
    long buildTime;
    long lookupTime;
  }

  @Test
  public void testBenchmark() throws Exception {
    // this benchmark is very time consuming
    boolean doTest = false;
    if (!doTest) {
      return;
    }
    Map<String,Integer> ref = new HashMap<String,Integer>();
    JaspellLookup jaspell = new JaspellLookup();
    TSTLookup tst = new TSTLookup();
    
    _benchmark(tst, ref, true, null);
    _benchmark(jaspell, ref, true, null);
    jaspell = null;
    tst = null;
    int count = 100;
    Bench b = runBenchmark(JaspellLookup.class, count);
    System.err.println(JaspellLookup.class.getSimpleName() + ": buildTime[ms]=" + (b.buildTime / count) +
            " lookupTime[ms]=" + (b.lookupTime / count / 1000000));
    b = runBenchmark(TSTLookup.class, count);
    System.err.println(TSTLookup.class.getSimpleName() + ": buildTime[ms]=" + (b.buildTime / count) +
            " lookupTime[ms]=" + (b.lookupTime / count / 1000000));
  }
  
  private Bench runBenchmark(Class<? extends Lookup> cls, int count) throws Exception {
    System.err.println("* Running " + count + " iterations for " + cls.getSimpleName() + " ...");
    System.err.println("  - warm-up 10 iterations...");
    for (int i = 0; i < 10; i++) {
      System.runFinalization();
      System.gc();
      Lookup lookup = cls.newInstance();
      _benchmark(lookup, null, false, null);
      lookup = null;
    }
    Bench b = new Bench();
    System.err.print("  - main iterations:"); System.err.flush();
    for (int i = 0; i < count; i++) {
      System.runFinalization();
      System.gc();
      Lookup lookup = cls.newInstance();
      _benchmark(lookup, null, false, b);
      lookup = null;
      if (i > 0 && (i % 10 == 0)) {
        System.err.print(" " + i);
        System.err.flush();
      }
    }
    System.err.println();
    return b;
  }
}
