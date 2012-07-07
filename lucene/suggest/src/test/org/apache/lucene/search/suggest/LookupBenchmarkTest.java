package org.apache.lucene.search.suggest;

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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.lucene.util.*;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.fst.FSTCompletionLookup;
import org.apache.lucene.search.suggest.fst.WFSTCompletionLookup;
import org.apache.lucene.search.suggest.jaspell.JaspellLookup;
import org.apache.lucene.search.suggest.tst.TSTLookup;

import org.junit.BeforeClass;
import org.junit.Ignore;

/**
 * Benchmarks tests for implementations of {@link Lookup} interface.
 */
@Ignore("COMMENT ME TO RUN BENCHMARKS!")
public class LookupBenchmarkTest extends LuceneTestCase {
  @SuppressWarnings("unchecked")
  private final List<Class<? extends Lookup>> benchmarkClasses = Arrays.asList(
      JaspellLookup.class, 
      TSTLookup.class,
      FSTCompletionLookup.class,
      WFSTCompletionLookup.class);

  private final static int rounds = 15;
  private final static int warmup = 5;

  private final int num = 7;
  private final boolean onlyMorePopular = true;

  private final static Random random = new Random(0xdeadbeef);

  /**
   * Input term/weight pairs.
   */
  private static TermFreq [] dictionaryInput;

  /**
   * Benchmark term/weight pairs (randomized order).
   */
  private static List<TermFreq> benchmarkInput;

  /**
   * Loads terms and frequencies from Wikipedia (cached).
   */
  @BeforeClass
  public static void setup() throws Exception {
    assert false : "disable assertions before running benchmarks!";
    List<TermFreq> input = readTop50KWiki();
    Collections.shuffle(input, random);
    LookupBenchmarkTest.dictionaryInput = input.toArray(new TermFreq [input.size()]);
    Collections.shuffle(input, random);
    LookupBenchmarkTest.benchmarkInput = input;
  }

  static final Charset UTF_8 = Charset.forName("UTF-8");

  /**
   * Collect the multilingual input for benchmarks/ tests.
   */
  public static List<TermFreq> readTop50KWiki() throws Exception {
    List<TermFreq> input = new ArrayList<TermFreq>();
    URL resource = LookupBenchmarkTest.class.getResource("Top50KWiki.utf8");
    assert resource != null : "Resource missing: Top50KWiki.utf8";

    String line = null;
    BufferedReader br = new BufferedReader(new InputStreamReader(resource.openStream(), UTF_8));
    while ((line = br.readLine()) != null) {
      int tab = line.indexOf('|');
      assertTrue("No | separator?: " + line, tab >= 0);
      int weight = Integer.parseInt(line.substring(tab + 1));
      String key = line.substring(0, tab);
      input.add(new TermFreq(key, weight));
    }
    br.close();
    return input;
  }

  /**
   * Test construction time.
   */
  public void testConstructionTime() throws Exception {
    System.err.println("-- construction time");
    for (final Class<? extends Lookup> cls : benchmarkClasses) {
      BenchmarkResult result = measure(new Callable<Integer>() {
        public Integer call() throws Exception {
          final Lookup lookup = buildLookup(cls, dictionaryInput);          
          return lookup.hashCode();
        }
      });

      System.err.println(
          String.format(Locale.ROOT, "%-15s input: %d, time[ms]: %s",
              cls.getSimpleName(),
              dictionaryInput.length,
              result.average.toString()));
    }
  }

  /**
   * Test memory required for the storage.
   */
  public void testStorageNeeds() throws Exception {
    System.err.println("-- RAM consumption");
    for (Class<? extends Lookup> cls : benchmarkClasses) {
      Lookup lookup = buildLookup(cls, dictionaryInput);
      System.err.println(
          String.format(Locale.ROOT, "%-15s size[B]:%,13d",
              lookup.getClass().getSimpleName(), 
              RamUsageEstimator.sizeOf(lookup)));
    }
  }

  /**
   * Create {@link Lookup} instance and populate it. 
   */
  private Lookup buildLookup(Class<? extends Lookup> cls, TermFreq[] input) throws Exception {
    Lookup lookup = cls.newInstance();
    lookup.build(new TermFreqArrayIterator(input));
    return lookup;
  }

  /**
   * Test performance of lookup on full hits.
   */
  public void testPerformanceOnFullHits() throws Exception {
    final int minPrefixLen = 100;
    final int maxPrefixLen = 200;
    runPerformanceTest(minPrefixLen, maxPrefixLen, num, onlyMorePopular);
  }

  /**
   * Test performance of lookup on longer term prefixes (6-9 letters or shorter).
   */
  public void testPerformanceOnPrefixes6_9() throws Exception {
    final int minPrefixLen = 6;
    final int maxPrefixLen = 9;
    runPerformanceTest(minPrefixLen, maxPrefixLen, num, onlyMorePopular);
  }

  /**
   * Test performance of lookup on short term prefixes (2-4 letters or shorter).
   */
  public void testPerformanceOnPrefixes2_4() throws Exception {
    final int minPrefixLen = 2;
    final int maxPrefixLen = 4;
    runPerformanceTest(minPrefixLen, maxPrefixLen, num, onlyMorePopular);
  }

  /**
   * Run the actual benchmark. 
   */
  public void runPerformanceTest(final int minPrefixLen, final int maxPrefixLen, 
      final int num, final boolean onlyMorePopular) throws Exception {
    System.err.println(String.format(Locale.ROOT,
        "-- prefixes: %d-%d, num: %d, onlyMorePopular: %s",
        minPrefixLen, maxPrefixLen, num, onlyMorePopular));

    for (Class<? extends Lookup> cls : benchmarkClasses) {
      final Lookup lookup = buildLookup(cls, dictionaryInput);

      final List<String> input = new ArrayList<String>(benchmarkInput.size());
      for (TermFreq tf : benchmarkInput) {
        String s = tf.term.utf8ToString();
        input.add(s.substring(0, Math.min(s.length(), 
              minPrefixLen + random.nextInt(maxPrefixLen - minPrefixLen + 1))));
      }

      BenchmarkResult result = measure(new Callable<Integer>() {
        public Integer call() throws Exception {
          int v = 0;
          for (String term : input) {
            v += lookup.lookup(term, onlyMorePopular, num).size();
          }
          return v;
        }
      });

      System.err.println(
          String.format(Locale.ROOT, "%-15s queries: %d, time[ms]: %s, ~kQPS: %.0f",
              lookup.getClass().getSimpleName(),
              input.size(),
              result.average.toString(),
              input.size() / result.average.avg));
    }
  }

  /**
   * Do the measurements.
   */
  private BenchmarkResult measure(Callable<Integer> callable) {
    final double NANOS_PER_MS = 1000000;

    try {
      List<Double> times = new ArrayList<Double>();
      for (int i = 0; i < warmup + rounds; i++) {
          final long start = System.nanoTime();
          guard = callable.call().intValue();
          times.add((System.nanoTime() - start) / NANOS_PER_MS);
      }
      return new BenchmarkResult(times, warmup, rounds);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Guard against opts. */
  @SuppressWarnings("unused")
  private static volatile int guard;

  private static class BenchmarkResult {
    /** Average time per round (ms). */
    public final Average average;

    public BenchmarkResult(List<Double> times, int warmup, int rounds) {
      this.average = Average.from(times.subList(warmup, times.size()));
    }
  }
}