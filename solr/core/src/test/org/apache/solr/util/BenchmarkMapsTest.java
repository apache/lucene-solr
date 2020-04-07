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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Random;

import com.carrotsearch.hppc.HashContainers;
import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.util.numeric.IntIntDynamicMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkMapsTest extends LuceneTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final static long seed = System.nanoTime();

  private static void run(String file, Consumer consumer) throws IOException {
    int[] maxSizes = new int[]{1 << 16, 1 << 18, 1 << 20, 1 << 24};
    double[] percents = new double[]{0.001, 0.005, 0.01, 0.015, 0.03, 0.06};
    int[] numRuns = new int[]{20000, 10000, 500, 200};
    Random random = new Random(seed);
    BufferedWriter writer = new BufferedWriter(new FileWriter(new File("/Users/caomanhdat/temp/"+file)));
    writer.write("size_maxSize_percent,time\n");
    for (int i = 0; i < maxSizes.length; i++) {
      int maxSize = maxSizes[i];
      FixedBitSet bitSet = new FixedBitSet(maxSize);
      for (double percent: percents) {
        long t = System.currentTimeMillis();
        int size = (int) (maxSize * percent);
        for (int j = 0; j < numRuns[i]; j++) {
          consumer.consume(size, maxSize, bitSet, random);
        }
        log.info("maxSize:{} size:{} percent:{} time:{}", maxSize, size, percent, System.currentTimeMillis() - t);
        writer.write(String.format("%d_%d,%d\n", size, maxSize, (System.currentTimeMillis() - t)));
      }
    }
    writer.close();
  }

  interface Consumer {
    void consume(int size, int maxSize, FixedBitSet bitSet, Random random);
  }

//  public void testBlockMaps() {
//    run((size, maxSize, bitSet, rand) -> {
//      IntIntBlockArrayBasedMap map = new IntIntBlockArrayBasedMap(maxSize, -1);
//      for (int i = 0; i < size; i++) {
//        int k = rand.nextInt(maxSize);
//        map.set(k, k);
//      }
//      map.forEachValue(bitSet::set);
//    });
//  }

  public void testDynamicMaps() throws IOException {
    run("dynamic.csv", (size, maxSize, bitSet, rand) -> {
      IntIntDynamicMap map = new IntIntDynamicMap(maxSize, -1);
      for (int i = 0; i < size; i++) {
        map.set(i, i);
      }
      map.forEachValue(bitSet::set);
    });
  }

  public void testMaps() throws IOException {
    run("hppc.csv", (size, maxSize, bitSet, rand) -> {
      IntIntHashMap map = new IntIntHashMap((int) ((maxSize >>> 5) / HashContainers.DEFAULT_LOAD_FACTOR));
      for (int i = 0; i < size; i++) {
        int k = rand.nextInt(maxSize);
        map.put(i, i);
      }
      for (IntCursor cursor: map.values()) {
        bitSet.set(cursor.value);
      }
    });
  }

  public void testArray() throws IOException {
    run("array.csv", (size, maxSize, bitSet, rand) -> {
      int[] arr = new int[maxSize];
      Arrays.fill(arr, -1);
      for (int i = 0; i < size; i++) {
        arr[i] = i;
      }
      for (int val: arr) {
        if (val != -1)
          bitSet.set(val);
      }
    });
  }

//  public static void main(String[] args) {
//    BenchmarkMaps benchmark = new BenchmarkMaps();
//    benchmark.testMaps();
//  }
}
