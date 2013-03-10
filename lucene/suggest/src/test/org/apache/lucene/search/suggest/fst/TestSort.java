package org.apache.lucene.search.suggest.fst;

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

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.search.suggest.Sort;
import org.apache.lucene.search.suggest.Sort.BufferSize;
import org.apache.lucene.search.suggest.Sort.ByteSequencesWriter;
import org.apache.lucene.search.suggest.Sort.SortInfo;
import org.apache.lucene.util.*;
import org.junit.*;

/**
 * Tests for on-disk merge sorting.
 */
public class TestSort extends LuceneTestCase {
  private File tempDir;

  @Before
  public void prepareTempDir() throws IOException {
    tempDir = _TestUtil.getTempDir("mergesort");
    _TestUtil.rmDir(tempDir);
    tempDir.mkdirs();
  }
  
  @After
  public void cleanup() throws IOException {
    if (tempDir != null)
      _TestUtil.rmDir(tempDir);
  }

  @Test
  public void testEmpty() throws Exception {
    checkSort(new Sort(), new byte [][] {});
  }

  @Test
  public void testSingleLine() throws Exception {
    checkSort(new Sort(), new byte [][] {
        "Single line only.".getBytes("UTF-8")
    });
  }

  @Test
  public void testIntermediateMerges() throws Exception {
    // Sort 20 mb worth of data with 1mb buffer, binary merging.
    SortInfo info = checkSort(new Sort(Sort.DEFAULT_COMPARATOR, BufferSize.megabytes(1), Sort.defaultTempDir(), 2), 
        generateRandom((int)Sort.MB * 20));
    assertTrue(info.mergeRounds > 10);
  }

  @Test
  public void testSmallRandom() throws Exception {
    // Sort 20 mb worth of data with 1mb buffer.
    SortInfo sortInfo = checkSort(new Sort(Sort.DEFAULT_COMPARATOR, BufferSize.megabytes(1), Sort.defaultTempDir(), Sort.MAX_TEMPFILES), 
        generateRandom((int)Sort.MB * 20));
    assertEquals(1, sortInfo.mergeRounds);
  }

  @Test @Nightly
  public void testLargerRandom() throws Exception {
    // Sort 100MB worth of data with 15mb buffer.
    checkSort(new Sort(Sort.DEFAULT_COMPARATOR, BufferSize.megabytes(16), Sort.defaultTempDir(), Sort.MAX_TEMPFILES), 
        generateRandom((int)Sort.MB * 100));
  }

  private byte[][] generateRandom(int howMuchData) {
    ArrayList<byte[]> data = new ArrayList<byte[]>(); 
    while (howMuchData > 0) {
      byte [] current = new byte [random().nextInt(256)];
      random().nextBytes(current);
      data.add(current);
      howMuchData -= current.length;
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
   * Check sorting data on an instance of {@link Sort}.
   */
  private SortInfo checkSort(Sort sort, byte[][] data) throws IOException {
    File unsorted = writeAll("unsorted", data);

    Arrays.sort(data, unsignedByteOrderComparator);
    File golden = writeAll("golden", data);

    File sorted = new File(tempDir, "sorted");
    SortInfo sortInfo = sort.sort(unsorted, sorted);
    //System.out.println("Input size [MB]: " + unsorted.length() / (1024 * 1024));
    //System.out.println(sortInfo);

    assertFilesIdentical(golden, sorted);
    return sortInfo;
  }

  /**
   * Make sure two files are byte-byte identical.
   */
  private void assertFilesIdentical(File golden, File sorted) throws IOException {
    assertEquals(golden.length(), sorted.length());

    byte [] buf1 = new byte [64 * 1024];
    byte [] buf2 = new byte [64 * 1024];
    int len;
    DataInputStream is1 = new DataInputStream(new FileInputStream(golden));
    DataInputStream is2 = new DataInputStream(new FileInputStream(sorted));
    while ((len = is1.read(buf1)) > 0) {
      is2.readFully(buf2, 0, len);
      for (int i = 0; i < len; i++) {
        assertEquals(buf1[i], buf2[i]);
      }
    }
    IOUtils.close(is1, is2);
  }

  private File writeAll(String name, byte[][] data) throws IOException {
    File file = new File(tempDir, name);
    ByteSequencesWriter w = new Sort.ByteSequencesWriter(file);
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
}
