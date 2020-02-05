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
package org.apache.lucene.util.packed;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestDirectMonotonic extends LuceneTestCase {

  public void testValidation() {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
        () -> DirectMonotonicWriter.getInstance(null, null, -1, 10));
    assertEquals("numValues can't be negative, got -1", e.getMessage());

    e = expectThrows(IllegalArgumentException.class,
        () -> DirectMonotonicWriter.getInstance(null, null, 10, 1));
    assertEquals("blockShift must be in [2-22], got 1", e.getMessage());

    e = expectThrows(IllegalArgumentException.class,
        () -> DirectMonotonicWriter.getInstance(null, null, 1L << 40, 5));
    assertEquals("blockShift is too low for the provided number of values: blockShift=5, numValues=1099511627776, MAX_ARRAY_LENGTH=" +
        ArrayUtil.MAX_ARRAY_LENGTH, e.getMessage());
  }

  public void testEmpty() throws IOException {
    Directory dir = newDirectory();
    final int blockShift = TestUtil.nextInt(random(), DirectMonotonicWriter.MIN_BLOCK_SHIFT, DirectMonotonicWriter.MAX_BLOCK_SHIFT);

    final long dataLength;
    try (IndexOutput metaOut = dir.createOutput("meta", IOContext.DEFAULT);
        IndexOutput dataOut = dir.createOutput("data", IOContext.DEFAULT)) {
      DirectMonotonicWriter w = DirectMonotonicWriter.getInstance(metaOut, dataOut, 0, blockShift);
      w.finish();
      dataLength = dataOut.getFilePointer();
    }

    try (IndexInput metaIn = dir.openInput("meta", IOContext.READONCE);
        IndexInput dataIn = dir.openInput("data", IOContext.DEFAULT)) {
      DirectMonotonicReader.Meta meta = DirectMonotonicReader.loadMeta(metaIn, 0, blockShift);
      DirectMonotonicReader.getInstance(meta, dataIn.randomAccessSlice(0, dataLength));
      // no exception
    }

    dir.close();
  }

  public void testSimple() throws IOException {
    Directory dir = newDirectory();
    final int blockShift = 2;

    List<Long> actualValues = Arrays.asList(1L, 2L, 5L, 7L, 8L, 100L);
    final int numValues = actualValues.size();

    final long dataLength;
    try (IndexOutput metaOut = dir.createOutput("meta", IOContext.DEFAULT);
        IndexOutput dataOut = dir.createOutput("data", IOContext.DEFAULT)) {
      DirectMonotonicWriter w = DirectMonotonicWriter.getInstance(metaOut, dataOut, numValues, blockShift);
      for (long v : actualValues) {
        w.add(v);
      }
      w.finish();
      dataLength = dataOut.getFilePointer();
    }

    try (IndexInput metaIn = dir.openInput("meta", IOContext.READONCE);
        IndexInput dataIn = dir.openInput("data", IOContext.DEFAULT)) {
      DirectMonotonicReader.Meta meta = DirectMonotonicReader.loadMeta(metaIn, numValues, blockShift);
      LongValues values = DirectMonotonicReader.getInstance(meta, dataIn.randomAccessSlice(0, dataLength));
      for (int i = 0; i < numValues; ++i) {
        final long v = values.get(i);
        assertEquals(actualValues.get(i).longValue(), v);
      }
    }

    dir.close();
  }

  public void testConstantSlope() throws IOException {
    Directory dir = newDirectory();
    final int blockShift = TestUtil.nextInt(random(), DirectMonotonicWriter.MIN_BLOCK_SHIFT, DirectMonotonicWriter.MAX_BLOCK_SHIFT);
    final int numValues = TestUtil.nextInt(random(), 1, 1 << 20);
    final long min = random().nextLong();
    final long inc = random().nextInt(1 << random().nextInt(20));

    List<Long> actualValues = new ArrayList<>();
    for (int i = 0; i < numValues; ++i) {
      actualValues.add(min + inc * i);
    }

    final long dataLength;
    try (IndexOutput metaOut = dir.createOutput("meta", IOContext.DEFAULT);
        IndexOutput dataOut = dir.createOutput("data", IOContext.DEFAULT)) {
      DirectMonotonicWriter w = DirectMonotonicWriter.getInstance(metaOut, dataOut, numValues, blockShift);
      for (long v : actualValues) {
        w.add(v);
      }
      w.finish();
      dataLength = dataOut.getFilePointer();
    }

    try (IndexInput metaIn = dir.openInput("meta", IOContext.READONCE);
        IndexInput dataIn = dir.openInput("data", IOContext.DEFAULT)) {
      DirectMonotonicReader.Meta meta = DirectMonotonicReader.loadMeta(metaIn, numValues, blockShift);
      LongValues values = DirectMonotonicReader.getInstance(meta, dataIn.randomAccessSlice(0, dataLength));
      for (int i = 0; i < numValues; ++i) {
        assertEquals(actualValues.get(i).longValue(), values.get(i));
      }
      assertEquals(0, dataIn.getFilePointer());
    }

    dir.close();
  }

  public void testRandom() throws IOException {
    Random random = random();
    final int iters = atLeast(random, 3);
    for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();
      final int blockShift = TestUtil.nextInt(random, DirectMonotonicWriter.MIN_BLOCK_SHIFT, DirectMonotonicWriter.MAX_BLOCK_SHIFT);
      final int maxNumValues = 1 << 20;
      final int numValues;
      if (random.nextBoolean()) {
        // random number
        numValues = TestUtil.nextInt(random, 1, maxNumValues);
      } else {
        // multiple of the block size
        final int numBlocks = TestUtil.nextInt(random, 0, maxNumValues >>> blockShift);
        numValues = TestUtil.nextInt(random, 0, numBlocks) << blockShift;
      }
      List<Long> actualValues = new ArrayList<>();
      long previous = random.nextLong();
      if (numValues > 0) {
        actualValues.add(previous);
      }
      for (int i = 1; i < numValues; ++i) {
        previous += random.nextInt(1 << random.nextInt(20));
        actualValues.add(previous);
      }
  
      final long dataLength;
      try (IndexOutput metaOut = dir.createOutput("meta", IOContext.DEFAULT);
          IndexOutput dataOut = dir.createOutput("data", IOContext.DEFAULT)) {
        DirectMonotonicWriter w = DirectMonotonicWriter.getInstance(metaOut, dataOut, numValues, blockShift);
        for (long v : actualValues) {
          w.add(v);
        }
        w.finish();
        dataLength = dataOut.getFilePointer();
      }
  
      try (IndexInput metaIn = dir.openInput("meta", IOContext.READONCE);
          IndexInput dataIn = dir.openInput("data", IOContext.DEFAULT)) {
        DirectMonotonicReader.Meta meta = DirectMonotonicReader.loadMeta(metaIn, numValues, blockShift);
        LongValues values = DirectMonotonicReader.getInstance(meta, dataIn.randomAccessSlice(0, dataLength));
        for (int i = 0; i < numValues; ++i) {
          assertEquals(actualValues.get(i).longValue(), values.get(i));
        }
      }
  
      dir.close();
    }
  }

  public void testMonotonicBinarySearch() throws IOException {
    try (Directory dir = newDirectory()) {
      doTestMonotonicBinarySearchAgainstLongArray(dir, new long[] {4, 7, 8, 10, 19, 30, 55, 78, 100}, 2);
    }
  }

  public void testMonotonicBinarySearchRandom() throws IOException {
    try (Directory dir = newDirectory()) {
      final int iters = atLeast(100);
      for (int iter = 0; iter < iters; ++iter) {
        final int arrayLength = random().nextInt(1 << random().nextInt(14));
        final long[] array = new long[arrayLength];
        final long base = random().nextLong();
        final int bpv = TestUtil.nextInt(random(), 4, 61);
        for (int i = 0; i < array.length; ++i) {
          array[i] = base + TestUtil.nextLong(random(), 0, (1L << bpv) - 1);
        }
        Arrays.sort(array);
        doTestMonotonicBinarySearchAgainstLongArray(dir, array, TestUtil.nextInt(random(), 2, 10));
      }
    }
  }

  private void doTestMonotonicBinarySearchAgainstLongArray(Directory dir, long[] array, int blockShift) throws IOException {
    try (IndexOutput metaOut = dir.createOutput("meta", IOContext.DEFAULT);
        IndexOutput dataOut = dir.createOutput("data", IOContext.DEFAULT)) {
      DirectMonotonicWriter writer = DirectMonotonicWriter.getInstance(metaOut, dataOut, array.length, blockShift);
      for (long l : array) {
        writer.add(l);
      }
      writer.finish();
    }

    try (IndexInput metaIn = dir.openInput("meta", IOContext.READONCE);
        IndexInput dataIn = dir.openInput("data", IOContext.READ)) {
      DirectMonotonicReader.Meta meta = DirectMonotonicReader.loadMeta(metaIn, array.length, blockShift);
      DirectMonotonicReader reader = DirectMonotonicReader.getInstance(meta, dataIn.randomAccessSlice(0L, dir.fileLength("data")));

      if (array.length == 0) {
        assertEquals(-1, reader.binarySearch(0, array.length, 42L));
      } else {
        for (int i = 0; i < array.length; ++i) {
          final long index = reader.binarySearch(0, array.length, array[i]);
          assertTrue(index >= 0);
          assertTrue(index < array.length);
          assertEquals(array[i], array[(int) index]);
        }
        if (array[0] != Long.MIN_VALUE) {
          assertEquals(-1, reader.binarySearch(0, array.length, array[0] - 1));
        }
        if (array[array.length - 1] != Long.MAX_VALUE) {
          assertEquals(-1 - array.length, reader.binarySearch(0, array.length, array[array.length - 1] + 1));
        }
        for (int i = 0; i < array.length - 2; ++i) {
          if (array[i] + 1 < array[i+1]) {
            final long intermediate = random().nextBoolean() ? array[i] + 1 : array[i+1] - 1;
            final long index = reader.binarySearch(0, array.length, intermediate);
            assertTrue(index < 0);
            final int insertionPoint = Math.toIntExact(-1 -index);
            assertTrue(insertionPoint > 0);
            assertTrue(insertionPoint < array.length);
            assertTrue(array[insertionPoint] > intermediate);
            assertTrue(array[insertionPoint-1] < intermediate);
          }
        }
      }
    }
    dir.deleteFile("meta");
    dir.deleteFile("data");
  }
}
