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


import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.CorruptingIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.OfflineSorter.BufferSize;
import org.apache.lucene.util.OfflineSorter.ByteSequencesWriter;
import org.apache.lucene.util.OfflineSorter.SortInfo;

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
    try (Directory dir = newDirectory()) {
        checkSort(dir, new OfflineSorter(dir, "foo"), new byte [][] {});
    }
  }

  public void testSingleLine() throws Exception {
    try (Directory dir = newDirectory()) {
      checkSort(dir, new OfflineSorter(dir, "foo"), new byte [][] {
          "Single line only.".getBytes(StandardCharsets.UTF_8)
        });
    }
  }

  private ExecutorService randomExecutorServiceOrNull() {
    if (random().nextBoolean()) {
      return null;
    } else {
      return new ThreadPoolExecutor(1, TestUtil.nextInt(random(), 2, 6), Long.MAX_VALUE, TimeUnit.MILLISECONDS,
                                    new LinkedBlockingQueue<Runnable>(),
                                    new NamedThreadFactory("TestIndexSearcher"));
    }
  }

  public void testIntermediateMerges() throws Exception {
    // Sort 20 mb worth of data with 1mb buffer, binary merging.
    try (Directory dir = newDirectory()) {
      ExecutorService exec = randomExecutorServiceOrNull();
      SortInfo info = checkSort(dir, new OfflineSorter(dir, "foo", OfflineSorter.DEFAULT_COMPARATOR, BufferSize.megabytes(1), 2, -1, exec, TestUtil.nextInt(random(), 1, 4)),
                                generateRandom((int)OfflineSorter.MB * 20));
      if (exec != null) {
        exec.shutdownNow();
      }
      assertTrue(info.mergeRounds > 10);
    }
  }

  public void testSmallRandom() throws Exception {
    // Sort 20 mb worth of data with 1mb buffer.
    try (Directory dir = newDirectory()) {
      ExecutorService exec = randomExecutorServiceOrNull();
      SortInfo sortInfo = checkSort(dir, new OfflineSorter(dir, "foo", OfflineSorter.DEFAULT_COMPARATOR, BufferSize.megabytes(1), OfflineSorter.MAX_TEMPFILES, -1, exec, TestUtil.nextInt(random(), 1, 4)),
                                    generateRandom((int)OfflineSorter.MB * 20));
      if (exec != null) {
        exec.shutdownNow();
      }
      assertEquals(3, sortInfo.mergeRounds);
    }
  }

  @Nightly
  public void testLargerRandom() throws Exception {
    // Sort 100MB worth of data with 15mb buffer.
    try (Directory dir = newFSDirectory(createTempDir())) {
      ExecutorService exec = randomExecutorServiceOrNull();
      checkSort(dir, new OfflineSorter(dir, "foo", OfflineSorter.DEFAULT_COMPARATOR, BufferSize.megabytes(16), OfflineSorter.MAX_TEMPFILES, -1, exec, TestUtil.nextInt(random(), 1, 4)),
                generateRandom((int)OfflineSorter.MB * 100));
      if (exec != null) {
        exec.shutdownNow();
      }
    }
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

  // Generates same data every time:
  private byte[][] generateFixed(int howMuchDataInBytes) {
    ArrayList<byte[]> data = new ArrayList<>();
    int length = 256;
    byte counter = 0;
    while (howMuchDataInBytes > 0) {
      byte[] current = new byte[length];
      for(int i=0;i<current.length;i++) {
        current[i] = counter;
        counter++;
      }
      data.add(current);
      howMuchDataInBytes -= current.length;

      length--;
      if (length <= 128) {
        length = 256;
      }
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
        if (diff != 0) {
          return diff;
        }
      }
      return left.length - right.length;
    }
  };

  /**
   * Check sorting data on an instance of {@link OfflineSorter}.
   */
  private SortInfo checkSort(Directory dir, OfflineSorter sorter, byte[][] data) throws IOException {

    IndexOutput unsorted = dir.createTempOutput("unsorted", "tmp", IOContext.DEFAULT);
    writeAll(unsorted, data);

    IndexOutput golden = dir.createTempOutput("golden", "tmp", IOContext.DEFAULT);
    Arrays.sort(data, unsignedByteOrderComparator);
    writeAll(golden, data);

    String sorted = sorter.sort(unsorted.getName());
    //System.out.println("Input size [MB]: " + unsorted.length() / (1024 * 1024));
    //System.out.println(sortInfo);
    assertFilesIdentical(dir, golden.getName(), sorted);

    return sorter.sortInfo;
  }

  /**
   * Make sure two files are byte-byte identical.
   */
  private void assertFilesIdentical(Directory dir, String golden, String sorted) throws IOException {
    long numBytes = dir.fileLength(golden);
    assertEquals(numBytes, dir.fileLength(sorted));

    byte[] buf1 = new byte[64 * 1024];
    byte[] buf2 = new byte[64 * 1024];
    try (
         IndexInput in1 = dir.openInput(golden, IOContext.READONCE);
         IndexInput in2 = dir.openInput(sorted, IOContext.READONCE)
         ) {
      long left = numBytes;
      while (left > 0) {
        int chunk = (int) Math.min(buf1.length, left);
        left -= chunk;
        in1.readBytes(buf1, 0, chunk);
        in2.readBytes(buf2, 0, chunk);
        for (int i = 0; i < chunk; i++) {
          assertEquals(buf1[i], buf2[i]);
        }
      }
    }
  }

  /** NOTE: closes the provided {@link IndexOutput} */
  private void writeAll(IndexOutput out, byte[][] data) throws IOException {
    try (ByteSequencesWriter w = new OfflineSorter.ByteSequencesWriter(out)) {
      for (byte [] datum : data) {
        w.write(datum);
      }
      CodecUtil.writeFooter(out);
    }
  }
  
  public void testRamBuffer() {
    int numIters = atLeast(10000);
    for (int i = 0; i < numIters; i++) {
      BufferSize.megabytes(1+random().nextInt(2047));
    }
    BufferSize.megabytes(2047);
    BufferSize.megabytes(1);
    
    expectThrows(IllegalArgumentException.class, () -> {
      BufferSize.megabytes(2048);
    });
    
    expectThrows(IllegalArgumentException.class, () -> {
      BufferSize.megabytes(0);
    });
    
    expectThrows(IllegalArgumentException.class, () -> {
      BufferSize.megabytes(-1);
    });
  }

  public void testThreadSafety() throws Exception {
    Thread[] threads = new Thread[TestUtil.nextInt(random(), 4, 10)];
    final AtomicBoolean failed = new AtomicBoolean();
    final int iters = atLeast(1000);
    try (Directory dir = newDirectory()) {
      for(int i=0;i<threads.length;i++) {
        final int threadID = i;
        threads[i] = new Thread() {
            @Override
            public void run() {
              try {
                for(int iter=0;iter<iters && failed.get() == false;iter++) {
                  checkSort(dir, new OfflineSorter(dir, "foo_" + threadID + "_" + iter), generateRandom(1024));
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
    }

    assertFalse(failed.get());
  }

  /** Make sure corruption on the incoming (unsorted) file is caught, even if the corruption didn't confuse OfflineSorter! */
  public void testBitFlippedOnInput1() throws Exception {

    try (Directory dir0 = newMockDirectory()) {

      Directory dir = new FilterDirectory(dir0) {
        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
          IndexOutput out = in.createTempOutput(prefix, suffix, context);
          if (prefix.equals("unsorted")) {
            return new CorruptingIndexOutput(dir0, 22, out);
          } else {
            return out;
          }
        }
      };

      IndexOutput unsorted = dir.createTempOutput("unsorted", "tmp", IOContext.DEFAULT);
      writeAll(unsorted, generateFixed(10*1024));

      CorruptIndexException e = expectThrows(CorruptIndexException.class, () -> {
          new OfflineSorter(dir, "foo").sort(unsorted.getName());
        });
      assertTrue(e.getMessage().contains("checksum failed (hardware problem?)"));
    }
  }

  /** Make sure corruption on the incoming (unsorted) file is caught, if the corruption did confuse OfflineSorter! */
  public void testBitFlippedOnInput2() throws Exception {

    try (Directory dir0 = newMockDirectory()) {

      Directory dir = new FilterDirectory(dir0) {
        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
          IndexOutput out = in.createTempOutput(prefix, suffix, context);
          if (prefix.equals("unsorted")) {
            return new CorruptingIndexOutput(dir0, 22, out) {
              @Override
              protected void corruptFile() throws IOException {
                String newTempName;
                try(IndexOutput tmpOut = dir0.createTempOutput("tmp", "tmp", IOContext.DEFAULT);
                    IndexInput in = dir0.openInput(out.getName(), IOContext.DEFAULT)) {
                  newTempName = tmpOut.getName();
                  // Replace length at the end with a too-long value:
                  short v = in.readShort();
                  assertEquals(256, v);
                  tmpOut.writeShort(Short.MAX_VALUE);
                  tmpOut.copyBytes(in, in.length()-Short.BYTES);
                }

                // Delete original and copy corrupt version back:
                dir0.deleteFile(out.getName());
                dir0.copyFrom(dir0, newTempName, out.getName(), IOContext.DEFAULT);
                dir0.deleteFile(newTempName);
              }
            };
          } else {
            return out;
          }
        }
      };

      IndexOutput unsorted = dir.createTempOutput("unsorted", "tmp", IOContext.DEFAULT);
      writeAll(unsorted, generateFixed(5*1024));

      // This corruption made OfflineSorter fail with its own exception, but we verify it also went and added (as suppressed) that the
      // checksum was wrong:
      EOFException e = expectThrows(EOFException.class, () -> {
          new OfflineSorter(dir, "foo").sort(unsorted.getName());
        });
      assertEquals(1, e.getSuppressed().length);
      assertTrue(e.getSuppressed()[0] instanceof CorruptIndexException);
      assertTrue(e.getSuppressed()[0].getMessage().contains("checksum failed (hardware problem?)"));
    }
  }

  /** Make sure corruption on a temp file (partition) is caught, even if the corruption didn't confuse OfflineSorter! */
  public void testBitFlippedOnPartition1() throws Exception {

    try (Directory dir0 = newMockDirectory()) {

      Directory dir = new FilterDirectory(dir0) {

        boolean corrupted;

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
          IndexOutput out = in.createTempOutput(prefix, suffix, context);
          if (corrupted == false && suffix.equals("sort")) {
            corrupted = true;
            return new CorruptingIndexOutput(dir0, 544677, out);
          } else {
            return out;
          }
        }
      };

      IndexOutput unsorted = dir.createTempOutput("unsorted", "tmp", IOContext.DEFAULT);
      writeAll(unsorted, generateFixed((int) (OfflineSorter.MB * 3)));

      CorruptIndexException e = expectThrows(CorruptIndexException.class, () -> {
          new OfflineSorter(dir, "foo", OfflineSorter.DEFAULT_COMPARATOR, BufferSize.megabytes(1), 10, -1, null, 0).sort(unsorted.getName());
        });
      assertTrue(e.getMessage().contains("checksum failed (hardware problem?)"));
    }
  }

  /** Make sure corruption on a temp file (partition) is caught, if the corruption did confuse OfflineSorter! */
  public void testBitFlippedOnPartition2() throws Exception {

    try (Directory dir0 = newMockDirectory()) {

      Directory dir = new FilterDirectory(dir0) {

        boolean corrupted;

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
          IndexOutput out = in.createTempOutput(prefix, suffix, context);
          if (corrupted == false && suffix.equals("sort")) {
            corrupted = true;
            return new CorruptingIndexOutput(dir0, 544677, out) {
              @Override
              protected void corruptFile() throws IOException {
                String newTempName;
                try(IndexOutput tmpOut = dir0.createTempOutput("tmp", "tmp", IOContext.DEFAULT);
                    IndexInput in = dir0.openInput(out.getName(), IOContext.DEFAULT)) {
                  newTempName = tmpOut.getName();
                  tmpOut.copyBytes(in, 1025905);
                  short v = in.readShort();
                  assertEquals(254, v);
                  tmpOut.writeShort(Short.MAX_VALUE);
                  tmpOut.copyBytes(in, in.length()-1025905-Short.BYTES);
                }

                // Delete original and copy corrupt version back:
                dir0.deleteFile(out.getName());
                dir0.copyFrom(dir0, newTempName, out.getName(), IOContext.DEFAULT);
                dir0.deleteFile(newTempName);
              }
            };
          } else {
            return out;
          }
        }
      };

      IndexOutput unsorted = dir.createTempOutput("unsorted", "tmp", IOContext.DEFAULT);
      writeAll(unsorted, generateFixed((int) (OfflineSorter.MB * 3)));

      EOFException e = expectThrows(EOFException.class, () -> {
          new OfflineSorter(dir, "foo", OfflineSorter.DEFAULT_COMPARATOR, BufferSize.megabytes(1), 10, -1, null, 0).sort(unsorted.getName());
        });
      assertEquals(1, e.getSuppressed().length);
      assertTrue(e.getSuppressed()[0] instanceof CorruptIndexException);
      assertTrue(e.getSuppressed()[0].getMessage().contains("checksum failed (hardware problem?)"));
    }
  }

  public void testFixedLengthHeap() throws Exception {
    // Make sure the RAM accounting is correct, i.e. if we are sorting fixed width
    // ints (4 bytes) then the heap used is really only 4 bytes per value:
    Directory dir = newDirectory();
    IndexOutput out = dir.createTempOutput("unsorted", "tmp", IOContext.DEFAULT);
    try (ByteSequencesWriter w = new OfflineSorter.ByteSequencesWriter(out)) {
      byte[] bytes = new byte[Integer.BYTES];
      for (int i=0;i<1024*1024;i++) {
        random().nextBytes(bytes);
        w.write(bytes);
      }
      CodecUtil.writeFooter(out);
    }

    ExecutorService exec = randomExecutorServiceOrNull();
    OfflineSorter sorter = new OfflineSorter(dir, "foo", OfflineSorter.DEFAULT_COMPARATOR, BufferSize.megabytes(4), OfflineSorter.MAX_TEMPFILES, Integer.BYTES, exec, TestUtil.nextInt(random(), 1, 4));
    sorter.sort(out.getName());
    if (exec != null) {
      exec.shutdownNow();
    }
    // 1 MB of ints with 4 MH heap allowed should have been sorted in a single heap partition:
    assertEquals(0, sorter.sortInfo.mergeRounds);
    dir.close();
  }

  public void testFixedLengthLiesLiesLies() throws Exception {
    // Make sure OfflineSorter catches me if I lie about the fixed value length:
    Directory dir = newDirectory();
    IndexOutput out = dir.createTempOutput("unsorted", "tmp", IOContext.DEFAULT);
    try (ByteSequencesWriter w = new OfflineSorter.ByteSequencesWriter(out)) {
      byte[] bytes = new byte[Integer.BYTES];
      random().nextBytes(bytes);
      w.write(bytes);
      CodecUtil.writeFooter(out);
    }

    OfflineSorter sorter = new OfflineSorter(dir, "foo", OfflineSorter.DEFAULT_COMPARATOR, BufferSize.megabytes(4), OfflineSorter.MAX_TEMPFILES, Long.BYTES, null, 0);
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
      sorter.sort(out.getName());
      });
    assertEquals("value length is 4 but is supposed to always be 8", e.getMessage());
    dir.close();
  }

  // OfflineSorter should not call my BytesSequencesReader.next() again after it already returned null:
  public void testOverNexting() throws Exception {
    Directory dir = newDirectory();
    IndexOutput out = dir.createTempOutput("unsorted", "tmp", IOContext.DEFAULT);
    try (ByteSequencesWriter w = new OfflineSorter.ByteSequencesWriter(out)) {
      byte[] bytes = new byte[Integer.BYTES];
      random().nextBytes(bytes);
      w.write(bytes);
      CodecUtil.writeFooter(out);
    }

    new OfflineSorter(dir, "foo", OfflineSorter.DEFAULT_COMPARATOR, BufferSize.megabytes(4), OfflineSorter.MAX_TEMPFILES, Integer.BYTES, null, 0) {
      @Override
      protected ByteSequencesReader getReader(ChecksumIndexInput in, String name) throws IOException {
        ByteSequencesReader other = super.getReader(in, name);

        return new ByteSequencesReader(in, name) {

          private boolean alreadyEnded;
              
          @Override
          public BytesRef next() throws IOException {
            // if we returned null already, OfflineSorter should not call next() again
            assertFalse(alreadyEnded);
            BytesRef result = other.next();
            if (result == null) {
              alreadyEnded = true;
            }
            return result;
          }

          @Override
          public void close() throws IOException {
            other.close();
          }
        };
      }
    }.sort(out.getName());
    dir.close();
  }

  public void testInvalidFixedLength() throws Exception {
    IllegalArgumentException e;
    e = expectThrows(IllegalArgumentException.class,
                     () -> {
                       new OfflineSorter(null, "foo", OfflineSorter.DEFAULT_COMPARATOR,
                                         BufferSize.megabytes(1), OfflineSorter.MAX_TEMPFILES, 0, null, 0);
                     });
    assertEquals("valueLength must be 1 .. 32767; got: 0", e.getMessage());
    e = expectThrows(IllegalArgumentException.class,
                     () -> {
                       new OfflineSorter(null, "foo", OfflineSorter.DEFAULT_COMPARATOR,
                                         BufferSize.megabytes(1), OfflineSorter.MAX_TEMPFILES, Integer.MAX_VALUE, null, 0);
                     });
    assertEquals("valueLength must be 1 .. 32767; got: 2147483647", e.getMessage());
  }
}
