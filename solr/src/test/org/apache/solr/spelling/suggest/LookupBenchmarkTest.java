package org.apache.solr.spelling.suggest;

import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.spelling.suggest.fst.FSTLookup;
import org.apache.solr.spelling.suggest.jaspell.JaspellLookup;
import org.apache.solr.spelling.suggest.tst.TSTLookup;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;

/**
 * Benchmarks tests for implementations of {@link Lookup} interface.
 */
@Ignore // COMMENT ME TO RUN BENCHMARKS!
public class LookupBenchmarkTest {
  @SuppressWarnings("unchecked")
  private final List<Class<? extends Lookup>> benchmarkClasses = Lists.newArrayList(
      JaspellLookup.class, 
      TSTLookup.class,
      FSTLookup.class);

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
    List<TermFreq> input = readTop50KWiki();
    Collections.shuffle(input, random);
    LookupBenchmarkTest.dictionaryInput = input.toArray(new TermFreq [input.size()]);
    Collections.shuffle(input, random);
    LookupBenchmarkTest.benchmarkInput = input;
  }

  /**
   * Collect the multilingual input for benchmarks/ tests.
   */
  public static List<TermFreq> readTop50KWiki() throws Exception {
    List<TermFreq> input = Lists.newArrayList();
    URL resource = Thread.currentThread().getContextClassLoader().getResource("Top50KWiki.utf8");
    assert resource != null : "Resource missing: Top50KWiki.utf8";

    for (String line : Resources.readLines(resource, Charsets.UTF_8)) {
      int tab = line.indexOf('|');
      Assert.assertTrue("No | separator?: " + line, tab >= 0);
      float weight = Float.parseFloat(line.substring(tab + 1));
      String key = line.substring(0, tab);
      input.add(new TermFreq(key, weight));
    }
    return input;
  }

  /**
   * Test construction time.
   */
  @Test
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
          String.format(Locale.ENGLISH, "%-15s input: %d, time[ms]: %s",
              cls.getSimpleName(),
              dictionaryInput.length,
              result.average.toString()));
    }
  }

  /**
   * Test memory required for the storage.
   */
  @Test
  public void testStorageNeeds() throws Exception {
    System.err.println("-- RAM consumption");
    final RamUsageEstimator rue = new RamUsageEstimator();
    for (Class<? extends Lookup> cls : benchmarkClasses) {
      Lookup lookup = buildLookup(cls, dictionaryInput);
      System.err.println(
          String.format(Locale.ENGLISH, "%-15s size[B]:%,13d",
              lookup.getClass().getSimpleName(), 
              rue.estimateRamUsage(lookup)));
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
  @Test
  public void testPerformanceOnFullHits() throws Exception {
    final int minPrefixLen = 100;
    final int maxPrefixLen = 200;
    runPerformanceTest(minPrefixLen, maxPrefixLen, num, onlyMorePopular);
  }

  /**
   * Test performance of lookup on longer term prefixes (6-9 letters or shorter).
   */
  @Test
  public void testPerformanceOnPrefixes6_9() throws Exception {
    final int minPrefixLen = 6;
    final int maxPrefixLen = 9;
    runPerformanceTest(minPrefixLen, maxPrefixLen, num, onlyMorePopular);
  }

  /**
   * Test performance of lookup on short term prefixes (2-4 letters or shorter).
   */
  @Test
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
    System.err.println(String.format(Locale.ENGLISH,
        "-- prefixes: %d-%d, num: %d, onlyMorePopular: %s",
        minPrefixLen, maxPrefixLen, num, onlyMorePopular));

    for (Class<? extends Lookup> cls : benchmarkClasses) {
      final Lookup lookup = buildLookup(cls, dictionaryInput);

      final List<String> input = Lists.newArrayList(Iterables.transform(benchmarkInput, new Function<TermFreq, String>() {
        public String apply(TermFreq tf) {
          return tf.term.substring(0, Math.min(tf.term.length(), 
              minPrefixLen + random.nextInt(maxPrefixLen - minPrefixLen + 1)));
        }
      }));

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
          String.format(Locale.ENGLISH, "%-15s queries: %d, time[ms]: %s, ~qps: %.0f",
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
      List<Double> times = Lists.newArrayList();
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