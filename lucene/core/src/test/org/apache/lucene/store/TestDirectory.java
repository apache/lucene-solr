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


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase;

public class TestDirectory extends LuceneTestCase {

  // Test that different instances of FSDirectory can coexist on the same
  // path, can read, write, and lock files.
  public void testDirectInstantiation() throws Exception {
    final Path path = createTempDir("testDirectInstantiation");
    
    final byte[] largeBuffer = new byte[random().nextInt(256*1024)], largeReadBuffer = new byte[largeBuffer.length];
    for (int i = 0; i < largeBuffer.length; i++) {
      largeBuffer[i] = (byte) i; // automatically loops with modulo
    }

    final List<FSDirectory> dirs0 = new ArrayList<>();
    dirs0.add(new SimpleFSDirectory(path));
    dirs0.add(new NIOFSDirectory(path));
    if (hasWorkingMMapOnWindows()) {
      dirs0.add(new MMapDirectory(path));
    }
    final FSDirectory[] dirs = dirs0.stream().toArray(FSDirectory[]::new);

    for (int i=0; i<dirs.length; i++) {
      FSDirectory dir = dirs[i];
      dir.ensureOpen();
      String fname = "foo." + i;
      String lockname = "foo" + i + ".lck";
      IndexOutput out = dir.createOutput(fname, newIOContext(random()));
      out.writeByte((byte)i);
      out.writeBytes(largeBuffer, largeBuffer.length);
      out.close();

      for (int j=0; j<dirs.length; j++) {
        FSDirectory d2 = dirs[j];
        d2.ensureOpen();
        assertTrue(slowFileExists(d2, fname));
        assertEquals(1 + largeBuffer.length, d2.fileLength(fname));

        // don't do read tests if unmapping is not supported!
        if (d2 instanceof MMapDirectory && !((MMapDirectory) d2).getUseUnmap())
          continue;
        
        IndexInput input = d2.openInput(fname, newIOContext(random()));
        assertEquals((byte)i, input.readByte());
        // read array with buffering enabled
        Arrays.fill(largeReadBuffer, (byte)0);
        input.readBytes(largeReadBuffer, 0, largeReadBuffer.length, true);
        assertArrayEquals(largeBuffer, largeReadBuffer);
        // read again without using buffer
        input.seek(1L);
        Arrays.fill(largeReadBuffer, (byte)0);
        input.readBytes(largeReadBuffer, 0, largeReadBuffer.length, false);
        assertArrayEquals(largeBuffer, largeReadBuffer);        
        input.close();
      }

      // delete with a different dir
      dirs[(i+1)%dirs.length].deleteFile(fname);

      for (int j=0; j<dirs.length; j++) {
        FSDirectory d2 = dirs[j];
        assertFalse(slowFileExists(d2, fname));
      }

      Lock lock = dir.obtainLock(lockname);

      for (Directory other : dirs) {
        expectThrows(LockObtainFailedException.class, () -> {
          other.obtainLock(lockname);
        });
      }

      lock.close();
      
      // now lock with different dir
      lock = dirs[(i+1)%dirs.length].obtainLock(lockname);
      lock.close();
    }

    for (int i=0; i<dirs.length; i++) {
      FSDirectory dir = dirs[i];
      dir.ensureOpen();
      dir.close();
      assertFalse(dir.isOpen);
    }
  }

  // LUCENE-1468
  @SuppressWarnings("resource")
  public void testCopySubdir() throws Throwable {
    Path path = createTempDir("testsubdir");
    Files.createDirectory(path.resolve("subdir"));
    FSDirectory fsDir = new SimpleFSDirectory(path);
    RAMDirectory ramDir = new RAMDirectory(fsDir, newIOContext(random()));
    List<String> files = Arrays.asList(ramDir.listAll());
    assertFalse(files.contains("subdir"));
  }

  // LUCENE-1468
  public void testNotDirectory() throws Throwable {
    Path path = createTempDir("testnotdir");
    Directory fsDir = new SimpleFSDirectory(path);
    try {
      IndexOutput out = fsDir.createOutput("afile", newIOContext(random()));
      out.close();
      assertTrue(slowFileExists(fsDir, "afile"));
      expectThrows(IOException.class, () -> {
        new SimpleFSDirectory(path.resolve("afile"));
      });
    } finally {
      fsDir.close();
    }
  }
}

