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
package org.apache.lucene.util;

import java.util.Locale;
import java.util.Random;
import org.apache.lucene.util.BaseSortTestCase.Entry;
import org.apache.lucene.util.BaseSortTestCase.Strategy;

/**
 * Benchmark for {@link Sorter} implementations.
 *
 * <p>Run the static {@link #main(String[])} method to start the benchmark.
 */
public class SorterBenchmark {

  private static final int ARRAY_LENGTH = 20000;
  private static final int RUNS = 10;
  private static final int LOOPS = 100;

  private enum SorterFactory {
    INTRO_SORTER(
        "IntroSorter",
        (arr, s) -> {
          return new ArrayIntroSorter<>(arr, Entry::compareTo);
        }),
    TIM_SORTER(
        "TimSorter",
        (arr, s) -> {
          return new ArrayTimSorter<>(arr, Entry::compareTo, arr.length / 64);
        }),
    MERGE_SORTER(
        "MergeSorter",
        (arr, s) -> {
          return new ArrayInPlaceMergeSorter<>(arr, Entry::compareTo);
        }),
    ;
    final String name;
    final Builder builder;

    SorterFactory(String name, Builder builder) {
      this.name = name;
      this.builder = builder;
    }

    interface Builder {
      Sorter build(Entry[] arr, Strategy strategy);
    }
  }

  public static void main(String[] args) throws Exception {
    assert false : "Disable assertions to run the benchmark";
    Random random = new Random(System.currentTimeMillis());
    long seed = random.nextLong();

    System.out.println("WARMUP");
    benchmarkSorters(Strategy.RANDOM, random, seed);
    System.out.println();

    for (Strategy strategy : Strategy.values()) {
      System.out.println(strategy);
      benchmarkSorters(strategy, random, seed);
    }
  }

  private static void benchmarkSorters(Strategy strategy, Random random, long seed) {
    for (SorterFactory sorterFactory : SorterFactory.values()) {
      System.out.printf(Locale.ROOT, "  %-12s...", sorterFactory.name);
      random.setSeed(seed);
      benchmarkSorter(strategy, sorterFactory, random);
      System.out.println();
    }
  }

  private static void benchmarkSorter(
      Strategy strategy, SorterFactory sorterFactory, Random random) {
    for (int i = 0; i < RUNS; i++) {
      Entry[] original = createArray(strategy, random);
      Entry[] clone = original.clone();
      Sorter sorter = sorterFactory.builder.build(clone, strategy);
      long startTimeNs = System.nanoTime();
      for (int j = 0; j < LOOPS; j++) {
        System.arraycopy(original, 0, clone, 0, original.length);
        sorter.sort(0, clone.length);
      }
      long timeMs = (System.nanoTime() - startTimeNs) / 1000000;
      System.out.printf(Locale.ROOT, "%5d", timeMs);
    }
  }

  private static Entry[] createArray(Strategy strategy, Random random) {
    Entry[] arr = new Entry[ARRAY_LENGTH];
    for (int i = 0; i < arr.length; ++i) {
      strategy.set(arr, i, random);
    }
    return arr;
  }
}
