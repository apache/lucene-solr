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
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.RandomIndexWriter;

// See: https://issues.apache.org/jira/browse/SOLR-12028 Tests cannot remove files on Windows machines occasionally
public class TestMockDirectoryWrapper extends BaseDirectoryTestCase {
  
  @Override
  protected Directory getDirectory(Path path) throws IOException {
    final MockDirectoryWrapper dir;
    if (random().nextBoolean()) {
      dir = newMockDirectory();
    } else {
      dir = newMockFSDirectory(path);
    }
    return dir;
  }
  
  // we wrap the directory in slow stuff, so only run nightly
  @Override
  @Nightly
  public void testThreadSafetyInListAll() throws Exception {
    super.testThreadSafetyInListAll();
  }

  public void testDiskFull() throws IOException {
    // test writeBytes
    MockDirectoryWrapper dir = newMockDirectory();
    dir.setMaxSizeInBytes(3);
    final byte[] bytes = new byte[] { 1, 2};
    IndexOutput out = dir.createOutput("foo", IOContext.DEFAULT);
    out.writeBytes(bytes, bytes.length); // first write should succeed
    // close() to ensure the written bytes are not buffered and counted
    // against the directory size
    out.close();

    IndexOutput out2 = dir.createOutput("bar", IOContext.DEFAULT);
    expectThrows(IOException.class, () -> out2.writeBytes(bytes, bytes.length));
    out2.close();
    dir.close();
    
    // test copyBytes
    dir = newMockDirectory();
    dir.setMaxSizeInBytes(3);
    out = dir.createOutput("foo", IOContext.DEFAULT);
    out.copyBytes(new ByteArrayDataInput(bytes), bytes.length); // first copy should succeed
    // close() to ensure the written bytes are not buffered and counted
    // against the directory size
    out.close();

    IndexOutput out3 = dir.createOutput("bar", IOContext.DEFAULT);
    expectThrows(IOException.class, () -> out3.copyBytes(new ByteArrayDataInput(bytes), bytes.length));
    out3.close();
    dir.close();
  }
  
  public void testMDWinsideOfMDW() throws Exception {
    // add MDW inside another MDW
    Directory dir = new MockDirectoryWrapper(random(), newMockDirectory());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < 20; i++) {
      iw.addDocument(new Document());
    }
    iw.commit();
    iw.close();
    dir.close();
  }

  // just shields the wrapped directory from being closed
  private static class PreventCloseDirectoryWrapper extends FilterDirectory {
    public PreventCloseDirectoryWrapper(Directory in) {
      super(in);
    }

    @Override
    public void close() {
    }
  }

  public void testCorruptOnCloseIsWorkingFSDir() throws Exception {
    Path path = createTempDir();
    try(Directory dir = newFSDirectory(path)) {
      testCorruptOnCloseIsWorking(dir);
    }
  }

  public void testCorruptOnCloseIsWorkingOnByteBuffersDirectory() throws Exception {
    try(Directory dir = new ByteBuffersDirectory()) {
      testCorruptOnCloseIsWorking(dir);
    }
  }
    
  private void testCorruptOnCloseIsWorking(Directory dir) throws Exception {

    dir = new PreventCloseDirectoryWrapper(dir);

    try (MockDirectoryWrapper wrapped = new MockDirectoryWrapper(random(), dir)) {

      // otherwise MDW sometimes randomly leaves the file intact and we'll see false test failures:
      wrapped.alwaysCorrupt = true;

      // MDW will only try to corrupt things if it sees an index:
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      iw.addDocument(new Document());
      iw.close();
      
      // not sync'd!
      try (IndexOutput out = wrapped.createOutput("foo", IOContext.DEFAULT)) {
        for(int i=0;i<100;i++) {
          out.writeInt(i);
        }
      }

      // MDW.close now corrupts our unsync'd file (foo):
    }

    boolean changed = false;
    IndexInput in = null;
    try {
      in = dir.openInput("foo", IOContext.DEFAULT);
    } catch (NoSuchFileException | FileNotFoundException fnfe) {
      // ok
      changed = true;
    }
    if (in != null) {
      for(int i=0;i<100;i++) {
        int x;
        try {
          x = in.readInt();
        } catch (EOFException eofe) {
          changed = true;
          break;
        }
        if (x != i) {
          changed = true;
          break;
        }
      }

      in.close();
    }

    assertTrue("MockDirectoryWrapper on dir=" + dir + " failed to corrupt an unsync'd file", changed);
  }

  public void testAbuseClosedIndexInput() throws Exception {
    MockDirectoryWrapper dir = newMockDirectory();
    IndexOutput out = dir.createOutput("foo", IOContext.DEFAULT);
    out.writeByte((byte) 42);
    out.close();
    final IndexInput in = dir.openInput("foo", IOContext.DEFAULT);
    in.close();
    expectThrows(RuntimeException.class, in::readByte);
    dir.close();
  }

  public void testAbuseCloneAfterParentClosed() throws Exception {
    MockDirectoryWrapper dir = newMockDirectory();
    IndexOutput out = dir.createOutput("foo", IOContext.DEFAULT);
    out.writeByte((byte) 42);
    out.close();
    IndexInput in = dir.openInput("foo", IOContext.DEFAULT);
    final IndexInput clone = in.clone();
    in.close();
    expectThrows(RuntimeException.class, clone::readByte);
    dir.close();
  }

  public void testAbuseCloneOfCloneAfterParentClosed() throws Exception {
    MockDirectoryWrapper dir = newMockDirectory();
    IndexOutput out = dir.createOutput("foo", IOContext.DEFAULT);
    out.writeByte((byte) 42);
    out.close();
    IndexInput in = dir.openInput("foo", IOContext.DEFAULT);
    IndexInput clone1 = in.clone();
    IndexInput clone2 = clone1.clone();
    in.close();
    expectThrows(RuntimeException.class, clone2::readByte);
    dir.close();
  }
}
