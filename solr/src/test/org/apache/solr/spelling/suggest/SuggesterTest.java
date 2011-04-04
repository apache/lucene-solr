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

package org.apache.solr.spelling.suggest;

import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.SpellingParams;
import org.apache.solr.spelling.suggest.Lookup.LookupResult;
import org.apache.solr.spelling.suggest.jaspell.JaspellLookup;
import org.apache.solr.spelling.suggest.tst.TSTLookup;
import org.apache.solr.util.TermFreqIterator;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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
  
  @Test
  public void testReload() throws Exception {
    String leaveData = System.getProperty("solr.test.leavedatadir");
    if (leaveData == null) leaveData = "";
    System.setProperty("solr.test.leavedatadir", "true");
    addDocs();
    assertU(commit());
    File data = dataDir;
    String config = configString;
    deleteCore();
    dataDir = data;
    configString = config;
    initCore();
    assertQ(req("qt","/suggest", "q","ac", SpellingParams.SPELLCHECK_COUNT, "2", SpellingParams.SPELLCHECK_ONLY_MORE_POPULAR, "true"),
            "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/int[@name='numFound'][.='2']",
            "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/arr[@name='suggestion']/str[1][.='acquire']",
            "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/arr[@name='suggestion']/str[2][.='accommodate']"
        );
    
    // restore the property
    System.setProperty("solr.test.leavedatadir", leaveData);
  }
  
  @Test
  public void testRebuild() throws Exception {
    addDocs();
    assertU(commit());
    assertQ(req("qt","/suggest", "q","ac", SpellingParams.SPELLCHECK_COUNT, "2", SpellingParams.SPELLCHECK_ONLY_MORE_POPULAR, "true"),
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/int[@name='numFound'][.='2']");
    assertU(adoc("id", "4",
        "text", "actually"
       ));
    assertU(commit());
    assertQ(req("qt","/suggest", "q","ac", SpellingParams.SPELLCHECK_COUNT, "2", SpellingParams.SPELLCHECK_ONLY_MORE_POPULAR, "true"),
    "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/int[@name='numFound'][.='2']");
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
  
  static class Bench {
    long buildTime;
    long lookupTime;
  }

  @Test @Ignore
  public void testBenchmark() throws Exception {
    final List<Class<? extends Lookup>> benchmarkClasses = Lists.newArrayList();  
    benchmarkClasses.add(JaspellLookup.class);
    benchmarkClasses.add(TSTLookup.class);

    // Run a single pass just to see if everything works fine and provide size estimates.
    final RamUsageEstimator rue = new RamUsageEstimator();
    for (Class<? extends Lookup> cls : benchmarkClasses) {
      Lookup lookup = singleBenchmark(cls, null);
      System.err.println(
          String.format(Locale.ENGLISH,
              "%20s, size[B]=%,d",
              lookup.getClass().getSimpleName(), 
              rue.estimateRamUsage(lookup)));
    }

    int warmupCount = 10;
    int measuredCount = 100;
    for (Class<? extends Lookup> cls : benchmarkClasses) {
      Bench b = fullBenchmark(cls, warmupCount, measuredCount);
      System.err.println(String.format(Locale.ENGLISH,
          "%s: buildTime[ms]=%,d lookupTime[ms]=%,d",
          cls.getSimpleName(),
          (b.buildTime / measuredCount),
          (b.lookupTime / measuredCount / 1000000)));
    }
  }

  private Lookup singleBenchmark(Class<? extends Lookup> cls, Bench bench) throws Exception {
    Lookup lookup = cls.newInstance();

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
    }

    if (bench != null) {
      bench.buildTime += buildTime;
      bench.lookupTime +=  elapsed;
    }

    return lookup;
  }

  private Bench fullBenchmark(Class<? extends Lookup> cls, int warmupCount, int measuredCount) throws Exception {
    System.err.println("* Running " + measuredCount + " iterations for " + cls.getSimpleName() + " ...");
    System.err.println("  - warm-up " + warmupCount + " iterations...");
    for (int i = 0; i < warmupCount; i++) {
      System.runFinalization();
      System.gc();
      singleBenchmark(cls, null);
    }

    Bench b = new Bench();
    System.err.print("  - main iterations:"); System.err.flush();
    for (int i = 0; i < measuredCount; i++) {
      System.runFinalization();
      System.gc();
      singleBenchmark(cls, b);
      if (i > 0 && (i % 10 == 0)) {
        System.err.print(" " + i);
        System.err.flush();
      }
    }

    System.err.println();
    return b;
  }
}
