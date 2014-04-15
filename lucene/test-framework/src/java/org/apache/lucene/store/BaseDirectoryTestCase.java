package org.apache.lucene.store;

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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collections;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/** Base class for per-Directory tests. */

public abstract class BaseDirectoryTestCase extends LuceneTestCase {

  /** Subclass returns the Directory to be tested; if it's
   *  an FS-based directory it should point to the specified
   *  path, else it can ignore it. */
  protected abstract Directory getDirectory(File path) throws IOException;

  /** Make sure directory throws AlreadyClosedException if
   *  you try to createOutput after closing. */
  public void testDetectClose() throws Throwable {
    Directory dir = getDirectory(createTempDir("testDetectClose"));
    dir.close();
    try {
      dir.createOutput("test", newIOContext(random()));
      fail("did not hit expected exception");
    } catch (AlreadyClosedException ace) {
      // expected
    }
  }
  
  public void testThreadSafety() throws Exception {
    final Directory dir = getDirectory(createTempDir("testThreadSafety"));
    if (dir instanceof BaseDirectoryWrapper) {
      ((BaseDirectoryWrapper)dir).setCheckIndexOnClose(false); // we arent making an index
    }
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir).setThrottling(MockDirectoryWrapper.Throttling.NEVER); // makes this test really slow
    }
    
    if (VERBOSE) {
      System.out.println(dir);
    }

    class TheThread extends Thread {
      private String name;

      public TheThread(String name) {
        this.name = name;
      }
      
      @Override
      public void run() {
        for (int i = 0; i < 3000; i++) {
          String fileName = this.name + i;
          try {
            //System.out.println("create:" + fileName);
            IndexOutput output = dir.createOutput(fileName, newIOContext(random()));
            output.close();
            assertTrue(slowFileExists(dir, fileName));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    };
    
    class TheThread2 extends Thread {
      private String name;
      private volatile boolean stop;

      public TheThread2(String name) {
        this.name = name;
      }
      
      @Override
      public void run() {
        while (stop == false) {
          try {
            String[] files = dir.listAll();
            for (String file : files) {
              //System.out.println("file:" + file);
             try {
              IndexInput input = dir.openInput(file, newIOContext(random()));
              input.close();
              } catch (FileNotFoundException | NoSuchFileException e) {
                // ignore
              } catch (IOException e) {
                if (e.getMessage() != null && e.getMessage().contains("still open for writing")) {
                  // ignore
                } else {
                  throw new RuntimeException(e);
                }
              }
              if (random().nextBoolean()) {
                break;
              }
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    };
    
    TheThread theThread = new TheThread("t1");
    TheThread2 theThread2 = new TheThread2("t2");
    theThread.start();
    theThread2.start();
    
    theThread.join();
    
    // after first thread is done, no sense in waiting on thread 2 
    // to listFiles() and loop over and over
    theThread2.stop = true;
    theThread2.join();
    
    dir.close();
  }

  /** LUCENE-1464: just creating a Directory should not
   *  mkdir the underling directory in the filesystem. */
  public void testDontCreate() throws Throwable {
    File path = createTempDir("doesnotexist");
    TestUtil.rm(path);
    assertTrue(!path.exists());
    Directory dir = getDirectory(path);
    assertTrue(!path.exists());
    dir.close();
  }

  /** LUCENE-1468: once we create an output, we should see
   *  it in the dir listing and be able to open it with
   *  openInput. */
  public void testDirectoryFilter() throws IOException {
    String name = "file";
    Directory dir = getDirectory(createTempDir("testDirectoryFilter"));
    try {
      dir.createOutput(name, newIOContext(random())).close();
      assertTrue(slowFileExists(dir, name));
      assertTrue(Arrays.asList(dir.listAll()).contains(name));
    } finally {
      dir.close();
    }
  }

  // LUCENE-2852
  public void testSeekToEOFThenBack() throws Exception {
    Directory dir = getDirectory(createTempDir("testSeekToEOFThenBack"));

    IndexOutput o = dir.createOutput("out", newIOContext(random()));
    byte[] bytes = new byte[3*RAMInputStream.BUFFER_SIZE];
    o.writeBytes(bytes, 0, bytes.length);
    o.close();

    IndexInput i = dir.openInput("out", newIOContext(random()));
    i.seek(2*RAMInputStream.BUFFER_SIZE-1);
    i.seek(3*RAMInputStream.BUFFER_SIZE);
    i.seek(RAMInputStream.BUFFER_SIZE);
    i.readBytes(bytes, 0, 2*RAMInputStream.BUFFER_SIZE);
    i.close();
    dir.close();
  }

  // LUCENE-1196
  public void testIllegalEOF() throws Exception {
    Directory dir = getDirectory(createTempDir("testIllegalEOF"));
    IndexOutput o = dir.createOutput("out", newIOContext(random()));
    byte[] b = new byte[1024];
    o.writeBytes(b, 0, 1024);
    o.close();
    IndexInput i = dir.openInput("out", newIOContext(random()));
    i.seek(1024);
    i.close();
    dir.close();
  }

  public void testDeleteFile() throws Exception {
    Directory dir = getDirectory(createTempDir("testDeleteFile"));
    dir.createOutput("foo.txt", IOContext.DEFAULT).close();
    dir.deleteFile("foo.txt");
    assertEquals(0, dir.listAll().length);
    dir.close();
  }

  // LUCENE-3382 -- make sure we get exception if the directory really does not exist.
  public void testNoDir() throws Throwable {
    File tempDir = createTempDir("doesnotexist");
    TestUtil.rm(tempDir);
    Directory dir = getDirectory(tempDir);
    try {
      DirectoryReader.open(dir);
      fail("did not hit expected exception");
    } catch (NoSuchDirectoryException | IndexNotFoundException nsde) {
      // expected
    }
    dir.close();
  }

  // LUCENE-3382 test that delegate compound files correctly.
  public void testCompoundFileAppendTwice() throws IOException {
    Directory newDir = getDirectory(createTempDir("testCompoundFileAppendTwice"));
    CompoundFileDirectory csw = new CompoundFileDirectory(newDir, "d.cfs", newIOContext(random()), true);
    createSequenceFile(newDir, "d1", (byte) 0, 15);
    IndexOutput out = csw.createOutput("d.xyz", newIOContext(random()));
    out.writeInt(0);
    out.close();
    assertEquals(1, csw.listAll().length);
    assertEquals("d.xyz", csw.listAll()[0]);
   
    csw.close();

    CompoundFileDirectory cfr = new CompoundFileDirectory(newDir, "d.cfs", newIOContext(random()), false);
    assertEquals(1, cfr.listAll().length);
    assertEquals("d.xyz", cfr.listAll()[0]);
    cfr.close();
    newDir.close();
  }

  /** Creates a file of the specified size with sequential data. The first
   *  byte is written as the start byte provided. All subsequent bytes are
   *  computed as start + offset where offset is the number of the byte.
   */
  private void createSequenceFile(Directory dir, String name, byte start, int size) throws IOException {
    IndexOutput os = dir.createOutput(name, newIOContext(random()));
    for (int i=0; i < size; i++) {
      os.writeByte(start);
      start ++;
    }
    os.close();
  }

  public void testCopyBytes() throws Exception {
    testCopyBytes(getDirectory(createTempDir("testCopyBytes")));
  }

  private static byte value(int idx) {
    return (byte) ((idx % 256) * (1 + (idx / 256)));
  }
  
  public static void testCopyBytes(Directory dir) throws Exception {
      
    // make random file
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
      
    dir.close();
  }
  
  // LUCENE-3541
  public void testCopyBytesWithThreads() throws Exception {
    testCopyBytesWithThreads(getDirectory(createTempDir("testCopyBytesWithThreads")));
  }

  public static void testCopyBytesWithThreads(Directory d) throws Exception {
    int datalen = TestUtil.nextInt(random(), 101, 10000);
    byte data[] = new byte[datalen];
    random().nextBytes(data);
    
    IndexOutput output = d.createOutput("data", IOContext.DEFAULT);
    output.writeBytes(data, 0, datalen);
    output.close();
    
    IndexInput input = d.openInput("data", IOContext.DEFAULT);
    IndexOutput outputHeader = d.createOutput("header", IOContext.DEFAULT);
    // copy our 100-byte header
    outputHeader.copyBytes(input, 100);
    outputHeader.close();
    
    // now make N copies of the remaining bytes
    CopyThread copies[] = new CopyThread[10];
    for (int i = 0; i < copies.length; i++) {
      copies[i] = new CopyThread(input.clone(), d.createOutput("copy" + i, IOContext.DEFAULT));
    }
    
    for (int i = 0; i < copies.length; i++) {
      copies[i].start();
    }
    
    for (int i = 0; i < copies.length; i++) {
      copies[i].join();
    }
    
    for (int i = 0; i < copies.length; i++) {
      IndexInput copiedData = d.openInput("copy" + i, IOContext.DEFAULT);
      byte[] dataCopy = new byte[datalen];
      System.arraycopy(data, 0, dataCopy, 0, 100); // copy the header for easy testing
      copiedData.readBytes(dataCopy, 100, datalen-100);
      assertArrayEquals(data, dataCopy);
      copiedData.close();
    }
    input.close();
    d.close();
    
  }
  
  static class CopyThread extends Thread {
    final IndexInput src;
    final IndexOutput dst;
    
    CopyThread(IndexInput src, IndexOutput dst) {
      this.src = src;
      this.dst = dst;
    }

    @Override
    public void run() {
      try {
        dst.copyBytes(src, src.length()-100);
        dst.close();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }
}

