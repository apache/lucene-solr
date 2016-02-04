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


import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.util.OfflineSorter.BufferSize;
import org.apache.lucene.util.OfflineSorter.ByteSequencesWriter;
import org.apache.lucene.util.OfflineSorter.SortInfo;
import org.apache.lucene.util.OfflineSorter;

/**
 * Tests for on-disk merge sorting.
 */
public class TestOfflineSorter extends LuceneTestCase {
  private Path tempDir;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    tempDir = createTempDir("mergesort");
  }
  
  @Override
  public void tearDown() throws Exception {
    if (tempDir != null) {
      IOUtils.rm(tempDir);
    }
    super.tearDown();
  }

  public void testEmpty() throws Exception {
    checkSort(new OfflineSorter(), new byte [][] {});
  }

  public void testSingleLine() throws Exception {
    checkSort(new OfflineSorter(), new byte [][] {
        "Single line only.".getBytes(StandardCharsets.UTF_8)
    });
  }

  public void testIntermediateMerges() throws Exception {
    // Sort 20 mb worth of data with 1mb buffer, binary merging.
    SortInfo info = checkSort(new OfflineSorter(OfflineSorter.DEFAULT_COMPARATOR, BufferSize.megabytes(1), OfflineSorter.getDefaultTempDir(), 2), 
        generateRandom((int)OfflineSorter.MB * 20));
    assertTrue(info.mergeRounds > 10);
  }

  public void testSmallRandom() throws Exception {
    // Sort 20 mb worth of data with 1mb buffer.
    SortInfo sortInfo = checkSort(new OfflineSorter(OfflineSorter.DEFAULT_COMPARATOR, BufferSize.megabytes(1), OfflineSorter.getDefaultTempDir(), OfflineSorter.MAX_TEMPFILES), 
        generateRandom((int)OfflineSorter.MB * 20));
    assertEquals(1, sortInfo.mergeRounds);
  }

  @Nightly
  public void testLargerRandom() throws Exception {
    // Sort 100MB worth of data with 15mb buffer.
    checkSort(new OfflineSorter(OfflineSorter.DEFAULT_COMPARATOR, BufferSize.megabytes(16), OfflineSorter.getDefaultTempDir(), OfflineSorter.MAX_TEMPFILES), 
        generateRandom((int)OfflineSorter.MB * 100));
  }

  private byte[][] generateRandom(int howMuchDataInBytes) {
    ArrayList<byte[]> data = new ArrayList<>();
    while (howMuchDataInBytes > 0) {
      byte[] current = new byte[random().nextInt(256)];
      random().nextBytes(current);
      data.add(current);
      howMuchDataInBytes -= current.length;
    }
    byte [][] bytes = data.toArray(new byte[data.size()][]);
    return bytes;
  }
  
  static final Comparator<byte[]> unsignedByteOrderComparator = new Comparator<byte[]>() {
    @Override
    public int compare(byte[] left, byte[] right) {
      final int max = Math.min(left.length, right.length);
      for (int i = 0, j = 0; i < max; i++, j++) {
        int diff = (left[i]  & 0xff) - (right[j] & 0xff); 
        if (diff != 0) 
          return diff;
      }
      return left.length - right.length;
    }
  };

  /**
   * Check sorting data on an instance of {@link OfflineSorter}.
   */
  private SortInfo checkSort(OfflineSorter sort, byte[][] data) throws IOException {
    Path unsorted = writeAll("unsorted", data);

    Arrays.sort(data, unsignedByteOrderComparator);
    Path golden = writeAll("golden", data);

    Path sorted = Files.createTempFile(OfflineSorter.getDefaultTempDir(), "sorted", "");
    SortInfo sortInfo;
    try {
      sortInfo = sort.sort(unsorted, sorted);
      //System.out.println("Input size [MB]: " + unsorted.length() / (1024 * 1024));
      //System.out.println(sortInfo);
      assertFilesIdentical(golden, sorted);
    } finally {
      IOUtils.rm(unsorted, golden, sorted);
    }

    return sortInfo;
  }

  /**
   * Make sure two files are byte-byte identical.
   */
  private void assertFilesIdentical(Path golden, Path sorted) throws IOException {
    assertEquals(Files.size(golden), Files.size(sorted));

    byte [] buf1 = new byte [64 * 1024];
    byte [] buf2 = new byte [64 * 1024];
    int len;
    DataInputStream is1 = new DataInputStream(Files.newInputStream(golden));
    DataInputStream is2 = new DataInputStream(Files.newInputStream(sorted));
    while ((len = is1.read(buf1)) > 0) {
      is2.readFully(buf2, 0, len);
      for (int i = 0; i < len; i++) {
        assertEquals(buf1[i], buf2[i]);
      }
    }
    IOUtils.close(is1, is2);
  }

  private Path writeAll(String name, byte[][] data) throws IOException {
    Path file = Files.createTempFile(tempDir, name, "");
    ByteSequencesWriter w = new OfflineSorter.ByteSequencesWriter(file);
    for (byte [] datum : data) {
      w.write(datum);
    }
    w.close();
    return file;
  }
  
  public void testRamBuffer() {
    int numIters = atLeast(10000);
    for (int i = 0; i < numIters; i++) {
      BufferSize.megabytes(1+random().nextInt(2047));
    }
    BufferSize.megabytes(2047);
    BufferSize.megabytes(1);
    
    try {
      BufferSize.megabytes(2048);
      fail("max mb is 2047");
    } catch (IllegalArgumentException e) {
    }
    
    try {
      BufferSize.megabytes(0);
      fail("min mb is 0.5");
    } catch (IllegalArgumentException e) {
    }
    
    try {
      BufferSize.megabytes(-1);
      fail("min mb is 0.5");
    } catch (IllegalArgumentException e) {
    }
  }

  public void testThreadSafety() throws Exception {
    Thread[] threads = new Thread[TestUtil.nextInt(random(), 4, 10)];
    final AtomicBoolean failed = new AtomicBoolean();
    final int iters = atLeast(1000);
    for(int i=0;i<threads.length;i++) {
      threads[i] = new Thread() {
          @Override
          public void run() {
            try {
              for(int iter=0;iter<iters && failed.get() == false;iter++) {
                checkSort(new OfflineSorter(), generateRandom(1024));
              }
            } catch (Throwable th) {
              failed.set(true);
              throw new RuntimeException(th);
            }
          }
        };
      threads[i].start();
    }

    for(Thread thread : threads) {
      thread.join();
    }

    assertFalse(failed.get());
  }
}
