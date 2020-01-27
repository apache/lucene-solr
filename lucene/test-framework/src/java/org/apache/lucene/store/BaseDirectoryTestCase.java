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
package org.apache.lucene.store;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.CRC32;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.generators.RandomBytes;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.mockfile.ExtrasFS;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.Assert;

/**
 * Base class for per-Directory tests.
 */
public abstract class BaseDirectoryTestCase extends LuceneTestCase {

  /** A subclass returns the Directory to be tested; if it's
   *  an FS-based directory it should point to the specified
   *  path, else it can ignore it. */
  protected abstract Directory getDirectory(Path path) throws IOException;

  public void testCopyFrom() throws Exception {
    try (Directory source = getDirectory(createTempDir("testCopy"));
         Directory dest = newDirectory()) {
      runCopyFrom(source, dest);
    }

    try (Directory source = newDirectory();
         Directory dest = getDirectory(createTempDir("testCopyDestination"))) {
      runCopyFrom(source, dest);
    }
  }

  private void runCopyFrom(Directory source, Directory dest) throws IOException {
    IndexOutput output = source.createOutput("foobar", newIOContext(random()));

    byte bytes[] = RandomBytes.randomBytesOfLength(random(), 20000);
    output.writeBytes(bytes, bytes.length);
    output.close();

    dest.copyFrom(source, "foobar", "foobaz", newIOContext(random()));
    assertTrue(slowFileExists(dest, "foobaz"));

    IndexInput input = dest.openInput("foobaz", newIOContext(random()));
    byte bytes2[] = new byte[bytes.length];
    input.readBytes(bytes2, 0, bytes2.length);
    input.close();

    assertArrayEquals(bytes, bytes2);
  }

  public void testRename() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testRename"))) {
      IndexOutput output = dir.createOutput("foobar", newIOContext(random()));
      int numBytes = random().nextInt(20000);
      byte bytes[] = new byte[numBytes];
      random().nextBytes(bytes);
      output.writeBytes(bytes, bytes.length);
      output.close();

      dir.rename("foobar", "foobaz");

      IndexInput input = dir.openInput("foobaz", newIOContext(random()));
      byte bytes2[] = new byte[numBytes];
      input.readBytes(bytes2, 0, bytes2.length);
      assertEquals(input.length(), numBytes);
      input.close();

      assertArrayEquals(bytes, bytes2);
    }
  }

  public void testDeleteFile() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testDeleteFile"))) {
      String file = "foo.txt";
      Assert.assertFalse(Arrays.asList(dir.listAll()).contains(file));

      dir.createOutput("foo.txt", IOContext.DEFAULT).close();
      Assert.assertTrue(Arrays.asList(dir.listAll()).contains(file));

      dir.deleteFile("foo.txt");
      Assert.assertFalse(Arrays.asList(dir.listAll()).contains(file));

      expectThrowsAnyOf(Arrays.asList(NoSuchFileException.class, FileNotFoundException.class), () -> {
        dir.deleteFile("foo.txt");
      });
    }
  }

  public void testByte() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testByte"))) {
      IndexOutput output = dir.createOutput("byte", newIOContext(random()));
      output.writeByte((byte) 128);
      output.close();

      IndexInput input = dir.openInput("byte", newIOContext(random()));
      assertEquals(1, input.length());
      assertEquals((byte) 128, input.readByte());
      input.close();
    }
  }

  public void testShort() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testShort"))) {
      IndexOutput output = dir.createOutput("short", newIOContext(random()));
      output.writeShort((short) -20);
      output.close();

      IndexInput input = dir.openInput("short", newIOContext(random()));
      assertEquals(2, input.length());
      assertEquals((short) -20, input.readShort());
      input.close();
    }
  }

  public void testInt() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testInt"))) {
      IndexOutput output = dir.createOutput("int", newIOContext(random()));
      output.writeInt(-500);
      output.close();

      IndexInput input = dir.openInput("int", newIOContext(random()));
      assertEquals(4, input.length());
      assertEquals(-500, input.readInt());
      input.close();
    }
  }

  public void testLong() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testLong"))) {
      IndexOutput output = dir.createOutput("long", newIOContext(random()));
      output.writeLong(-5000);
      output.close();

      IndexInput input = dir.openInput("long", newIOContext(random()));
      assertEquals(8, input.length());
      assertEquals(-5000L, input.readLong());
      input.close();
    }
  }

  public void testAlignedLittleEndianLongs() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testAlignedLittleEndianLongs"))) {
      try (IndexOutput out = dir.createOutput("littleEndianLongs", newIOContext(random()))) {
        out.writeLong(Long.reverseBytes(3L));
        out.writeLong(Long.reverseBytes(Long.MAX_VALUE));
        out.writeLong(Long.reverseBytes(-3L));
      }
      try (IndexInput input = dir.openInput("littleEndianLongs", newIOContext(random()))) {
        assertEquals(24, input.length());
        long[] l = new long[4];
        input.readLELongs(l, 1, 3);
        assertArrayEquals(new long[] {0L, 3L, Long.MAX_VALUE, -3L}, l);
        assertEquals(24, input.getFilePointer());
      }
    }
  }

  public void testUnalignedLittleEndianLongs() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testUnalignedLittleEndianLongs"))) {
      try (IndexOutput out = dir.createOutput("littleEndianLongs", newIOContext(random()))) {
        out.writeByte((byte) 2);
        out.writeLong(Long.reverseBytes(3L));
        out.writeLong(Long.reverseBytes(Long.MAX_VALUE));
        out.writeLong(Long.reverseBytes(-3L));
      }
      try (IndexInput input = dir.openInput("littleEndianLongs", newIOContext(random()))) {
        assertEquals(25, input.length());
        assertEquals(2, input.readByte());
        long[] l = new long[4];
        input.readLELongs(l, 1, 3);
        assertArrayEquals(new long[] {0L, 3L, Long.MAX_VALUE, -3L}, l);
        assertEquals(25, input.getFilePointer());
      }
    }
  }

  public void testLittleEndianLongsUnderflow() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testLittleEndianLongsUnderflow"))) {
      final int offset = random().nextInt(8);
      final int length = TestUtil.nextInt(random(), 1, 16);
      try (IndexOutput out = dir.createOutput("littleEndianLongs", newIOContext(random()))) {
        byte[] b = new byte[offset + length * Long.BYTES - TestUtil.nextInt(random(), 1, Long.BYTES)];
        random().nextBytes(b);
        out.writeBytes(b, b.length);
      }
      try (IndexInput input = dir.openInput("littleEndianLongs", newIOContext(random()))) {
        input.seek(offset);
        expectThrows(EOFException.class,
            () -> input.readLELongs(new long[length], 0, length));
      }
    }
  }

  public void testString() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testString"))) {
      IndexOutput output = dir.createOutput("string", newIOContext(random()));
      output.writeString("hello!");
      output.close();

      IndexInput input = dir.openInput("string", newIOContext(random()));
      assertEquals("hello!", input.readString());
      assertEquals(7, input.length());
      input.close();
    }
  }

  public void testVInt() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testVInt"))) {
      IndexOutput output = dir.createOutput("vint", newIOContext(random()));
      output.writeVInt(500);
      output.close();

      IndexInput input = dir.openInput("vint", newIOContext(random()));
      assertEquals(2, input.length());
      assertEquals(500, input.readVInt());
      input.close();
    }
  }

  public void testVLong() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testVLong"))) {
      IndexOutput output = dir.createOutput("vlong", newIOContext(random()));
      output.writeVLong(Long.MAX_VALUE);
      output.close();

      IndexInput input = dir.openInput("vlong", newIOContext(random()));
      assertEquals(9, input.length());
      assertEquals(Long.MAX_VALUE, input.readVLong());
      input.close();
    }
  }

  public void testZInt() throws Exception {
    final int[] ints = new int[random().nextInt(10)];
    for (int i = 0; i < ints.length; ++i) {
      switch (random().nextInt(3)) {
        case 0:
          ints[i] = random().nextInt();
          break;
        case 1:
          ints[i] = random().nextBoolean() ? Integer.MIN_VALUE : Integer.MAX_VALUE;
          break;
        case 2:
          ints[i] = (random().nextBoolean() ? -1 : 1) * random().nextInt(1024);
          break;
        default:
          throw new AssertionError();
      }
    }

    try (Directory dir = getDirectory(createTempDir("testZInt"))) {
      IndexOutput output = dir.createOutput("zint", newIOContext(random()));
      for (int i : ints) {
        output.writeZInt(i);
      }
      output.close();

      IndexInput input = dir.openInput("zint", newIOContext(random()));
      for (int i : ints) {
        assertEquals(i, input.readZInt());
      }
      assertEquals(input.length(), input.getFilePointer());
      input.close();
    }
  }

  public void testZLong() throws Exception {
    final long[] longs = new long[random().nextInt(10)];
    for (int i = 0; i < longs.length; ++i) {
      switch (random().nextInt(3)) {
        case 0:
          longs[i] = random().nextLong();
          break;
        case 1:
          longs[i] = random().nextBoolean() ? Long.MIN_VALUE : Long.MAX_VALUE;
          break;
        case 2:
          longs[i] = (random().nextBoolean() ? -1 : 1) * random().nextInt(1024);
          break;
        default:
          throw new AssertionError();
      }
    }

    try (Directory dir = getDirectory(createTempDir("testZLong"))) {
      IndexOutput output = dir.createOutput("zlong", newIOContext(random()));
      for (long l : longs) {
        output.writeZLong(l);
      }
      output.close();

      IndexInput input = dir.openInput("zlong", newIOContext(random()));
      for (long l : longs) {
        assertEquals(l, input.readZLong());
      }
      assertEquals(input.length(), input.getFilePointer());
      input.close();
    }
  }

  public void testSetOfStrings() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testSetOfStrings"))) {

      IndexOutput output = dir.createOutput("stringset", newIOContext(random()));
      output.writeSetOfStrings(asSet("test1", "test2"));
      output.writeSetOfStrings(Collections.emptySet());
      output.writeSetOfStrings(asSet("test3"));
      output.close();

      IndexInput input = dir.openInput("stringset", newIOContext(random()));
      Set<String> set1 = input.readSetOfStrings();
      assertEquals(asSet("test1", "test2"), set1);
      // set should be immutable
      expectThrows(UnsupportedOperationException.class, () -> {
        set1.add("bogus");
      });

      Set<String> set2 = input.readSetOfStrings();
      assertEquals(Collections.emptySet(), set2);
      // set should be immutable
      expectThrows(UnsupportedOperationException.class, () -> {
        set2.add("bogus");
      });

      Set<String> set3 = input.readSetOfStrings();
      assertEquals(Collections.singleton("test3"), set3);
      // set should be immutable
      expectThrows(UnsupportedOperationException.class, () -> {
        set3.add("bogus");
      });

      assertEquals(input.length(), input.getFilePointer());
      input.close();
    }
  }

  public void testMapOfStrings() throws Exception {
    Map<String,String> m = new HashMap<>();
    m.put("test1", "value1");
    m.put("test2", "value2");

    try (Directory dir = getDirectory(createTempDir("testMapOfStrings"))) {
      IndexOutput output = dir.createOutput("stringmap", newIOContext(random()));
      output.writeMapOfStrings(m);
      output.writeMapOfStrings(Collections.emptyMap());
      output.writeMapOfStrings(Collections.singletonMap("key", "value"));
      output.close();

      IndexInput input = dir.openInput("stringmap", newIOContext(random()));
      Map<String, String> map1 = input.readMapOfStrings();
      assertEquals(m, map1);
      // map should be immutable
      expectThrows(UnsupportedOperationException.class, () -> {
        map1.put("bogus1", "bogus2");
      });

      Map<String, String> map2 = input.readMapOfStrings();
      assertEquals(Collections.emptyMap(), map2);
      // map should be immutable
      expectThrows(UnsupportedOperationException.class, () -> {
        map2.put("bogus1", "bogus2");
      });

      Map<String, String> map3 = input.readMapOfStrings();
      assertEquals(Collections.singletonMap("key", "value"), map3);
      // map should be immutable
      expectThrows(UnsupportedOperationException.class, () -> {
        map3.put("bogus1", "bogus2");
      });

      assertEquals(input.length(), input.getFilePointer());
      input.close();
    }
  }

  // TODO: fold in some of the testing of o.a.l.index.TestIndexInput in here!
  public void testChecksum() throws Exception {
    CRC32 expected = new CRC32();
    int numBytes = random().nextInt(20000);
    byte bytes[] = new byte[numBytes];
    random().nextBytes(bytes);
    expected.update(bytes);

    try (Directory dir = getDirectory(createTempDir("testChecksum"))) {
      IndexOutput output = dir.createOutput("checksum", newIOContext(random()));
      output.writeBytes(bytes, 0, bytes.length);
      output.close();

      ChecksumIndexInput input = dir.openChecksumInput("checksum", newIOContext(random()));
      input.skipBytes(numBytes);

      assertEquals(expected.getValue(), input.getChecksum());
      input.close();
    }
  }

  /** Make sure directory throws AlreadyClosedException if
   *  you try to createOutput after closing. */
  public void testDetectClose() throws Throwable {
    Directory dir = getDirectory(createTempDir("testDetectClose"));
    dir.close();

    expectThrows(AlreadyClosedException.class, () -> {
      dir.createOutput("test", newIOContext(random()));
    });
  }

  public void testThreadSafetyInListAll() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testThreadSafety"))) {
      if (dir instanceof BaseDirectoryWrapper) {
        // we are not making a real index, just writing, reading files.
        ((BaseDirectoryWrapper) dir).setCheckIndexOnClose(false);
      }
      if (dir instanceof MockDirectoryWrapper) {
        // makes this test really slow
        ((MockDirectoryWrapper) dir).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
      }

      AtomicBoolean stop = new AtomicBoolean();
      Thread writer = new Thread(() -> {
        try {
          Random rnd = new Random(RandomizedTest.randomLong() + 1);
          for (int i = 0, max = RandomizedTest.randomIntBetween(500, 1000); i < max; i++) {
            String fileName = "file-" + i;
            try (IndexOutput output = dir.createOutput(fileName, newIOContext(random()))) {
              assert output != null;
              // Add some lags so that the other thread can read the content of the directory.
              Thread.yield();
            }
            assertTrue(slowFileExists(dir, fileName));
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        } finally {
          stop.set(true);
        }
      });

      Thread reader = new Thread(() -> {
        try {
          Random rnd = new Random(RandomizedTest.randomLong());
          while (!stop.get()) {
            String [] files = Arrays.stream(dir.listAll())
                .filter(name -> !ExtrasFS.isExtra(name)) // Ignore anything from ExtraFS.
                .toArray(String[]::new);

            if (files.length > 0) {
              do {
                String file = RandomPicks.randomFrom(rnd, files);
                try (IndexInput input = dir.openInput(file, newIOContext(random()))) {
                  // Just open, nothing else.
                  assert input != null;
                } catch (AccessDeniedException e) {
                  // Access denied is allowed for files for which the output is still open (MockDirectoryWriter enforces
                  // this, for example). Since we don't synchronize with the writer thread, just ignore it.
                } catch (IOException e) {
                  throw new UncheckedIOException("Something went wrong when opening: " + file, e);
                }
              } while (rnd.nextInt(3) != 0); // Sometimes break and list files again.
            }
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      });

      reader.start();
      writer.start();

      writer.join();
      reader.join();
    }
  }

  /**
   * LUCENE-1468: once we create an output, we should see
   * it in the dir listing.
   */
  public void testFileExistsInListAfterCreated() throws IOException {
    try (Directory dir = getDirectory(createTempDir("testFileExistsInListAfterCreated"))) {
      String name = "file";
      dir.createOutput(name, newIOContext(random())).close();
      assertTrue(slowFileExists(dir, name));
      assertTrue(Arrays.asList(dir.listAll()).contains(name));
    }
  }

  // LUCENE-2852
  public void testSeekToEOFThenBack() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testSeekToEOFThenBack"))) {
      int bufferLength = 1024;
      IndexOutput o = dir.createOutput("out", newIOContext(random()));
      byte[] bytes = new byte[3 * bufferLength];
      o.writeBytes(bytes, 0, bytes.length);
      o.close();

      IndexInput i = dir.openInput("out", newIOContext(random()));
      i.seek(2 * bufferLength - 1);
      i.seek(3 * bufferLength);
      i.seek(bufferLength);
      i.readBytes(bytes, 0, 2 * bufferLength);
      i.close();
    }
  }

  // LUCENE-1196
  public void testIllegalEOF() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testIllegalEOF"))) {
      IndexOutput o = dir.createOutput("out", newIOContext(random()));
      byte[] b = new byte[1024];
      o.writeBytes(b, 0, 1024);
      o.close();
      IndexInput i = dir.openInput("out", newIOContext(random()));
      i.seek(1024);
      i.close();
    }
  }

  public void testSeekPastEOF() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testSeekPastEOF"))) {
      IndexOutput o = dir.createOutput("out", newIOContext(random()));
      final int len = random().nextInt(2048);
      byte[] b = new byte[len];
      o.writeBytes(b, 0, len);
      o.close();
      IndexInput i = dir.openInput("out", newIOContext(random()));

      // Seeking past EOF should always throw EOFException
      expectThrows(EOFException.class, () -> i.seek(len + RandomizedTest.randomIntBetween(1, 2048)));

      // Seeking exactly to EOF should never throw any exception.
      i.seek(len);

      // But any read following the seek(len) should throw an EOFException.
      expectThrows(EOFException.class, i::readByte);
      expectThrows(EOFException.class, () -> {
        i.readBytes(new byte [1], 0, 1);
      });

      i.close();
    }
  }

  public void testSliceOutOfBounds() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testSliceOutOfBounds"))) {
      IndexOutput o = dir.createOutput("out", newIOContext(random()));
      final int len = random().nextInt(2040) + 8;
      byte[] b = new byte[len];
      o.writeBytes(b, 0, len);
      o.close();
      IndexInput i = dir.openInput("out", newIOContext(random()));
      expectThrows(IllegalArgumentException.class, () -> {
        i.slice("slice1", 0, len + 1);
      });

      expectThrows(IllegalArgumentException.class, () -> {
        i.slice("slice2", -1, len);
      });

      IndexInput slice = i.slice("slice3", 4, len / 2);
      expectThrows(IllegalArgumentException.class, () -> {
        slice.slice("slice3sub", 1, len / 2);
      });

      i.close();
    }
  }

  // LUCENE-3382 -- make sure we get exception if the directory really does not exist.
  public void testNoDir() throws Throwable {
    Path tempDir = createTempDir("doesnotexist");
    IOUtils.rm(tempDir);
    try (Directory dir = getDirectory(tempDir)) {
      expectThrowsAnyOf(Arrays.asList(NoSuchFileException.class, IndexNotFoundException.class), () -> {
        DirectoryReader.open(dir);
      });
    }
  }

  public void testCopyBytes() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testCopyBytes"))) {
      IndexOutput out = dir.createOutput("test", newIOContext(random()));
      byte[] bytes = new byte[TestUtil.nextInt(random(), 1, 77777)];
      final int size = TestUtil.nextInt(random(), 1, 1777777);
      int upto = 0;
      int byteUpto = 0;
      while (upto < size) {
        bytes[byteUpto++] = value(upto);
        upto++;
        if (byteUpto == bytes.length) {
          out.writeBytes(bytes, 0, bytes.length);
          byteUpto = 0;
        }
      }

      out.writeBytes(bytes, 0, byteUpto);
      assertEquals(size, out.getFilePointer());
      out.close();
      assertEquals(size, dir.fileLength("test"));

      // copy from test -> test2
      final IndexInput in = dir.openInput("test", newIOContext(random()));

      out = dir.createOutput("test2", newIOContext(random()));

      upto = 0;
      while (upto < size) {
        if (random().nextBoolean()) {
          out.writeByte(in.readByte());
          upto++;
        } else {
          final int chunk = Math.min(
              TestUtil.nextInt(random(), 1, bytes.length), size - upto);
          out.copyBytes(in, chunk);
          upto += chunk;
        }
      }
      assertEquals(size, upto);
      out.close();
      in.close();

      // verify
      IndexInput in2 = dir.openInput("test2", newIOContext(random()));
      upto = 0;
      while (upto < size) {
        if (random().nextBoolean()) {
          final byte v = in2.readByte();
          assertEquals(value(upto), v);
          upto++;
        } else {
          final int limit = Math.min(
              TestUtil.nextInt(random(), 1, bytes.length), size - upto);
          in2.readBytes(bytes, 0, limit);
          for (int byteIdx = 0; byteIdx < limit; byteIdx++) {
            assertEquals(value(upto), bytes[byteIdx]);
            upto++;
          }
        }
      }
      in2.close();

      dir.deleteFile("test");
      dir.deleteFile("test2");
    }
  }

  private static byte value(int idx) {
    return (byte) ((idx % 256) * (1 + (idx / 256)));
  }

  // LUCENE-3541
  public void testCopyBytesWithThreads() throws Exception {
    try (Directory d = getDirectory(createTempDir("testCopyBytesWithThreads"))) {
      byte data[] = RandomBytes.randomBytesOfLengthBetween(random(), 101, 10000);

      IndexOutput output = d.createOutput("data", IOContext.DEFAULT);
      output.writeBytes(data, 0, data.length);
      output.close();

      IndexInput input = d.openInput("data", IOContext.DEFAULT);
      IndexOutput outputHeader = d.createOutput("header", IOContext.DEFAULT);
      // copy our 100-byte header
      outputHeader.copyBytes(input, 100);
      outputHeader.close();

      // now make N copies of the remaining bytes
      int threads = 10;
      CyclicBarrier start = new CyclicBarrier(threads);
      Thread copies [] = IntStream.range(0, threads)
          .mapToObj((i) -> {
            IndexInput src = input.clone();
            Thread t = new Thread(() -> {
              try {
                start.await();
                IndexOutput dst = d.createOutput("copy" + i, IOContext.DEFAULT);
                dst.copyBytes(src, src.length() - 100);
                dst.close();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
            t.start();
            return t;
          })
          .toArray(Thread[]::new);

      for (Thread t : copies) {
        t.join();
      }

      for (int i = 0; i < threads; i++) {
        try (IndexInput copiedData = d.openInput("copy" + i, IOContext.DEFAULT)) {
          byte[] dataCopy = new byte[data.length];
          System.arraycopy(data, 0, dataCopy, 0, 100);
          copiedData.readBytes(dataCopy, 100, data.length - 100);
          assertArrayEquals(data, dataCopy);
        }
      }
      input.close();
    }
  }

  // this test backdoors the directory via the filesystem. so it must actually use the filesystem
  // TODO: somehow change this test to
  public void testFsyncDoesntCreateNewFiles() throws Exception {
    Path path = createTempDir("nocreate");
    try (Directory fsdir = getDirectory(path)) {
      // this test backdoors the directory via the filesystem. so it must be an FSDir (for now)
      // TODO: figure a way to test this better/clean it up. E.g. we should be testing for FileSwitchDir,
      // if it's using two FSdirs and so on
      if (fsdir instanceof FSDirectory == false) {
        assumeTrue("test only works for FSDirectory subclasses", false);
        return;
      }

      // create a file
      IndexOutput out = fsdir.createOutput("afile", newIOContext(random()));
      out.writeString("boo");
      out.close();

      // delete it in the file system.
      Files.delete(path.resolve("afile"));

      int fileCount = fsdir.listAll().length;

      // fsync it
      expectThrowsAnyOf(Arrays.asList(FileNotFoundException.class, NoSuchFileException.class), () -> {
        fsdir.sync(Collections.singleton("afile"));
      });

      // no new files created
      assertEquals(fileCount, fsdir.listAll().length);
    }
  }
  
  // random access APIs

  public void testRandomLong() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testLongs"))) {
      IndexOutput output = dir.createOutput("longs", newIOContext(random()));
      int num = TestUtil.nextInt(random(), 50, 3000);
      long longs[] = new long[num];
      for (int i = 0; i < longs.length; i++) {
        longs[i] = TestUtil.nextLong(random(), Long.MIN_VALUE, Long.MAX_VALUE);
        output.writeLong(longs[i]);
      }
      output.close();

      // slice
      IndexInput input = dir.openInput("longs", newIOContext(random()));
      RandomAccessInput slice = input.randomAccessSlice(0, input.length());
      for (int i = 0; i < longs.length; i++) {
        assertEquals(longs[i], slice.readLong(i * 8));
      }

      // subslices
      for (int i = 1; i < longs.length; i++) {
        long offset = i * 8;
        RandomAccessInput subslice = input.randomAccessSlice(offset, input.length() - offset);
        for (int j = i; j < longs.length; j++) {
          assertEquals(longs[j], subslice.readLong((j - i) * 8));
        }
      }

      // with padding
      for (int i = 0; i < 7; i++) {
        String name = "longs-" + i;
        IndexOutput o = dir.createOutput(name, newIOContext(random()));
        byte junk[] = new byte[i];
        random().nextBytes(junk);
        o.writeBytes(junk, junk.length);
        input.seek(0);
        o.copyBytes(input, input.length());
        o.close();
        IndexInput padded = dir.openInput(name, newIOContext(random()));
        RandomAccessInput whole = padded.randomAccessSlice(i, padded.length() - i);
        for (int j = 0; j < longs.length; j++) {
          assertEquals(longs[j], whole.readLong(j * 8));
        }
        padded.close();
      }

      input.close();
    }
  }
  
  public void testRandomInt() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testInts"))) {
      IndexOutput output = dir.createOutput("ints", newIOContext(random()));
      int num = TestUtil.nextInt(random(), 50, 3000);
      int ints[] = new int[num];
      for (int i = 0; i < ints.length; i++) {
        ints[i] = random().nextInt();
        output.writeInt(ints[i]);
      }
      output.close();

      // slice
      IndexInput input = dir.openInput("ints", newIOContext(random()));
      RandomAccessInput slice = input.randomAccessSlice(0, input.length());
      for (int i = 0; i < ints.length; i++) {
        assertEquals(ints[i], slice.readInt(i * 4));
      }

      // subslices
      for (int i = 1; i < ints.length; i++) {
        long offset = i * 4;
        RandomAccessInput subslice = input.randomAccessSlice(offset, input.length() - offset);
        for (int j = i; j < ints.length; j++) {
          assertEquals(ints[j], subslice.readInt((j - i) * 4));
        }
      }

      // with padding
      for (int i = 0; i < 7; i++) {
        String name = "ints-" + i;
        IndexOutput o = dir.createOutput(name, newIOContext(random()));
        byte junk[] = new byte[i];
        random().nextBytes(junk);
        o.writeBytes(junk, junk.length);
        input.seek(0);
        o.copyBytes(input, input.length());
        o.close();
        IndexInput padded = dir.openInput(name, newIOContext(random()));
        RandomAccessInput whole = padded.randomAccessSlice(i, padded.length() - i);
        for (int j = 0; j < ints.length; j++) {
          assertEquals(ints[j], whole.readInt(j * 4));
        }
        padded.close();
      }
      input.close();
    }
  }
  
  public void testRandomShort() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testShorts"))) {
      IndexOutput output = dir.createOutput("shorts", newIOContext(random()));
      int num = TestUtil.nextInt(random(), 50, 3000);
      short shorts[] = new short[num];
      for (int i = 0; i < shorts.length; i++) {
        shorts[i] = (short) random().nextInt();
        output.writeShort(shorts[i]);
      }
      output.close();

      // slice
      IndexInput input = dir.openInput("shorts", newIOContext(random()));
      RandomAccessInput slice = input.randomAccessSlice(0, input.length());
      for (int i = 0; i < shorts.length; i++) {
        assertEquals(shorts[i], slice.readShort(i * 2));
      }

      // subslices
      for (int i = 1; i < shorts.length; i++) {
        long offset = i * 2;
        RandomAccessInput subslice = input.randomAccessSlice(offset, input.length() - offset);
        for (int j = i; j < shorts.length; j++) {
          assertEquals(shorts[j], subslice.readShort((j - i) * 2));
        }
      }

      // with padding
      for (int i = 0; i < 7; i++) {
        String name = "shorts-" + i;
        IndexOutput o = dir.createOutput(name, newIOContext(random()));
        byte junk[] = new byte[i];
        random().nextBytes(junk);
        o.writeBytes(junk, junk.length);
        input.seek(0);
        o.copyBytes(input, input.length());
        o.close();
        IndexInput padded = dir.openInput(name, newIOContext(random()));
        RandomAccessInput whole = padded.randomAccessSlice(i, padded.length() - i);
        for (int j = 0; j < shorts.length; j++) {
          assertEquals(shorts[j], whole.readShort(j * 2));
        }
        padded.close();
      }
      input.close();
    }
  }
  
  public void testRandomByte() throws Exception {
    try (Directory dir = getDirectory(createTempDir("testBytes"))) {
      IndexOutput output = dir.createOutput("bytes", newIOContext(random()));
      int num = TestUtil.nextInt(random(), 50, 3000);
      byte bytes[] = new byte[num];
      random().nextBytes(bytes);
      for (int i = 0; i < bytes.length; i++) {
        output.writeByte(bytes[i]);
      }
      output.close();

      // slice
      IndexInput input = dir.openInput("bytes", newIOContext(random()));
      RandomAccessInput slice = input.randomAccessSlice(0, input.length());
      for (int i = 0; i < bytes.length; i++) {
        assertEquals(bytes[i], slice.readByte(i));
      }

      // subslices
      for (int i = 1; i < bytes.length; i++) {
        long offset = i;
        RandomAccessInput subslice = input.randomAccessSlice(offset, input.length() - offset);
        for (int j = i; j < bytes.length; j++) {
          assertEquals(bytes[j], subslice.readByte(j - i));
        }
      }

      // with padding
      for (int i = 0; i < 7; i++) {
        String name = "bytes-" + i;
        IndexOutput o = dir.createOutput(name, newIOContext(random()));
        byte junk[] = new byte[i];
        random().nextBytes(junk);
        o.writeBytes(junk, junk.length);
        input.seek(0);
        o.copyBytes(input, input.length());
        o.close();
        IndexInput padded = dir.openInput(name, newIOContext(random()));
        RandomAccessInput whole = padded.randomAccessSlice(i, padded.length() - i);
        for (int j = 0; j < bytes.length; j++) {
          assertEquals(bytes[j], whole.readByte(j));
        }
        padded.close();
      }
      input.close();
    }
  }
  
  /** try to stress slices of slices */
  public void testSliceOfSlice() throws Exception {
    try (Directory dir = getDirectory(createTempDir("sliceOfSlice"))) {
      IndexOutput output = dir.createOutput("bytes", newIOContext(random()));
      final int num;
      if (TEST_NIGHTLY) {
        num = TestUtil.nextInt(random(), 250, 2500);
      } else {
        num = TestUtil.nextInt(random(), 50, 250);
      }
      byte bytes[] = new byte[num];
      random().nextBytes(bytes);
      for (int i = 0; i < bytes.length; i++) {
        output.writeByte(bytes[i]);
      }
      output.close();

      IndexInput input = dir.openInput("bytes", newIOContext(random()));
      // seek to a random spot shouldnt impact slicing.
      input.seek(TestUtil.nextLong(random(), 0, input.length()));
      for (int i = 0; i < num; i += 16) {
        IndexInput slice1 = input.slice("slice1", i, num - i);
        assertEquals(0, slice1.getFilePointer());
        assertEquals(num - i, slice1.length());

        // seek to a random spot shouldnt impact slicing.
        slice1.seek(TestUtil.nextLong(random(), 0, slice1.length()));
        for (int j = 0; j < slice1.length(); j += 16) {
          IndexInput slice2 = slice1.slice("slice2", j, num - i - j);
          assertEquals(0, slice2.getFilePointer());
          assertEquals(num - i - j, slice2.length());
          byte data[] = new byte[num];
          System.arraycopy(bytes, 0, data, 0, i + j);
          if (random().nextBoolean()) {
            // read the bytes for this slice-of-slice
            slice2.readBytes(data, i + j, num - i - j);
          } else {
            // seek to a random spot in between, read some, seek back and read the rest
            long seek = TestUtil.nextLong(random(), 0, slice2.length());
            slice2.seek(seek);
            slice2.readBytes(data, (int) (i + j + seek), (int) (num - i - j - seek));
            slice2.seek(0);
            slice2.readBytes(data, i + j, (int) seek);
          }
          assertArrayEquals(bytes, data);
        }
      }

      input.close();
    }
  }
  
  /** 
   * This test that writes larger than the size of the buffer output
   * will correctly increment the file pointer.
   */
  public void testLargeWrites() throws IOException {
    try (Directory dir = getDirectory(createTempDir("largeWrites"))) {
      IndexOutput os = dir.createOutput("testBufferStart.txt", newIOContext(random()));

      byte[] largeBuf = new byte[2048];
      random().nextBytes(largeBuf);

      long currentPos = os.getFilePointer();
      os.writeBytes(largeBuf, largeBuf.length);

      try {
        assertEquals(currentPos + largeBuf.length, os.getFilePointer());
      } finally {
        os.close();
      }
    }
  }

  // LUCENE-6084
  public void testIndexOutputToString() throws Throwable {
    try (Directory dir = getDirectory(createTempDir())) {
      IndexOutput out = dir.createOutput("camelCase.txt", newIOContext(random()));
      assertTrue(out.toString(), out.toString().contains("camelCase.txt"));
      out.close();
    }
  }
  
  public void testDoubleCloseOutput() throws Throwable {
    try (Directory dir = getDirectory(createTempDir())) {
      IndexOutput out = dir.createOutput("foobar", newIOContext(random()));
      out.writeString("testing");
      out.close();
      out.close(); // close again
    }
  }
  
  public void testDoubleCloseInput() throws Throwable {
    try (Directory dir = getDirectory(createTempDir())) {
      IndexOutput out = dir.createOutput("foobar", newIOContext(random()));
      out.writeString("testing");
      out.close();
      IndexInput in = dir.openInput("foobar", newIOContext(random()));
      assertEquals("testing", in.readString());
      in.close();
      in.close(); // close again
    }
  }

  public void testCreateTempOutput() throws Throwable {
    try (Directory dir = getDirectory(createTempDir())) {
      List<String> names = new ArrayList<>();
      int iters = atLeast(50);
      for (int iter = 0; iter < iters; iter++) {
        IndexOutput out = dir.createTempOutput("foo", "bar", newIOContext(random()));
        names.add(out.getName());
        out.writeVInt(iter);
        out.close();
      }
      for (int iter = 0; iter < iters; iter++) {
        IndexInput in = dir.openInput(names.get(iter), newIOContext(random()));
        assertEquals(iter, in.readVInt());
        in.close();
      }

      Set<String> files = Arrays.stream(dir.listAll())
          .filter(file -> !ExtrasFS.isExtra(file)) // remove any ExtrasFS stuff.
          .collect(Collectors.toSet());

      assertEquals(new HashSet<String>(names), files);
    }
  }

  public void testCreateOutputForExistingFile() throws IOException {
    try (Directory dir = getDirectory(createTempDir())) {
      String name = "file";
      try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
        assert out != null;
      }

      // Try to create an existing file should fail.
      expectThrows(FileAlreadyExistsException.class, () -> {
        try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
          assert out != null;
        }
      });

      // Delete file and try to recreate it.
      dir.deleteFile(name);
      dir.createOutput(name, IOContext.DEFAULT).close();
    }
  }

  public void testSeekToEndOfFile() throws IOException {
    try (Directory dir = getDirectory(createTempDir())) {
      try (IndexOutput out = dir.createOutput("a", IOContext.DEFAULT)) {
        for (int i = 0; i < 1024; ++i) {
          out.writeByte((byte) 0);
        }
      }
      try (IndexInput in = dir.openInput("a", IOContext.DEFAULT)) {
        in.seek(100);
        assertEquals(100, in.getFilePointer());
        in.seek(1024);
        assertEquals(1024, in.getFilePointer());
      }
    }
  }

  public void testSeekBeyondEndOfFile() throws IOException {
    try (Directory dir = getDirectory(createTempDir())) {
      try (IndexOutput out = dir.createOutput("a", IOContext.DEFAULT)) {
        for (int i = 0; i < 1024; ++i) {
          out.writeByte((byte) 0);
        }
      }
      try (IndexInput in = dir.openInput("a", IOContext.DEFAULT)) {
        in.seek(100);
        assertEquals(100, in.getFilePointer());
        expectThrows(EOFException.class, () -> {      
          in.seek(1025);
        });
      }
    }
  }

  // Make sure the FSDirectory impl properly "emulates" deletions on filesystems (Windows) with buggy deleteFile:
  public void testPendingDeletions() throws IOException {
    try (Directory dir = getDirectory(addVirusChecker(createTempDir()))) {
      assumeTrue("we can only install VirusCheckingFS on an FSDirectory", dir instanceof FSDirectory);
      FSDirectory fsDir = (FSDirectory) dir;

      // Keep trying until virus checker refuses to delete:
      final String fileName;
      while (true) {
        // create a random filename (segment file name style), so it cannot hit windows problem with special filenames ("con", "com1",...):
        String candidate = IndexFileNames.segmentFileName(TestUtil.randomSimpleString(random(), 1, 6), TestUtil.randomSimpleString(random()), "test");
        try (IndexOutput out = dir.createOutput(candidate, IOContext.DEFAULT)) {
          out.getFilePointer(); // just fake access to prevent compiler warning
        }
        fsDir.deleteFile(candidate);
        if (fsDir.getPendingDeletions().size() > 0) {
          // good: virus checker struck and prevented deletion of fileName
          fileName = candidate;
          break;
        }
      }

      // Make sure listAll does NOT include the file:
      assertFalse(Arrays.asList(fsDir.listAll()).contains(fileName));

      // Make sure fileLength claims it's deleted:
      expectThrows(NoSuchFileException.class, () -> {      
        fsDir.fileLength(fileName);
      });

      // Make sure rename fails:
      expectThrows(NoSuchFileException.class, () -> {      
        fsDir.rename(fileName, "file2");
      });

      // Make sure delete fails:
      expectThrows(NoSuchFileException.class, () -> {      
        fsDir.deleteFile(fileName);
      });

      // Make sure we cannot open it for reading:
      expectThrows(NoSuchFileException.class, () -> {      
        fsDir.openInput(fileName, IOContext.DEFAULT);
      });
    }
  }

  public void testListAllIsSorted() throws IOException {
    try (Directory dir = getDirectory(createTempDir())) {
      int count = atLeast(20);
      Set<String> names = new HashSet<>();
      while (names.size() < count) {
        // create a random filename (segment file name style), so it cannot hit windows problem with special filenames ("con", "com1",...):
        String name = IndexFileNames.segmentFileName(TestUtil.randomSimpleString(random(), 1, 6), TestUtil.randomSimpleString(random()), "test");
        if (random().nextInt(5) == 1) {
          IndexOutput out = dir.createTempOutput(name, "foo", IOContext.DEFAULT);
          names.add(out.getName());
          out.close();
        } else if (names.contains(name) == false) {
          IndexOutput out = dir.createOutput(name, IOContext.DEFAULT);
          names.add(out.getName());
          out.close();
        }
      }
      String[] actual = dir.listAll();
      String[] expected = actual.clone();
      Arrays.sort(expected);
      assertArrayEquals(expected, actual);
    }
  }
}
