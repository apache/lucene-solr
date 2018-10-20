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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestDirectMonotonic extends LuceneTestCase {

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
    final int iters = atLeast(3);
    for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();
      final int blockShift = TestUtil.nextInt(random(), DirectMonotonicWriter.MIN_BLOCK_SHIFT, DirectMonotonicWriter.MAX_BLOCK_SHIFT);
      final int maxNumValues = 1 << 20;
      final int numValues;
      if (random().nextBoolean()) {
        // random number
        numValues = TestUtil.nextInt(random(), 1, maxNumValues);
      } else {
        // multiple of the block size
        final int numBlocks = TestUtil.nextInt(random(), 0, maxNumValues >>> blockShift);
        numValues = TestUtil.nextInt(random(), 0, numBlocks) << blockShift;
      }
      List<Long> actualValues = new ArrayList<>();
      long previous = random().nextLong();
      if (numValues > 0) {
        actualValues.add(previous);
      }
      for (int i = 1; i < numValues; ++i) {
        previous += random().nextInt(1 << random().nextInt(20));
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

}
