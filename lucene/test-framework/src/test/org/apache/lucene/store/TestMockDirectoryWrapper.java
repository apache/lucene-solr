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
import java.nio.file.Path;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase.Nightly;

public class TestMockDirectoryWrapper extends BaseDirectoryTestCase {
  
  @Override
  protected Directory getDirectory(Path path) throws IOException {
    final MockDirectoryWrapper dir;
    if (random().nextBoolean()) {
      dir = newMockDirectory();
    } else {
      dir = newMockFSDirectory(path);
    }
    dir.setEnableVirusScanner(false); // test manipulates filesystem directly
    return dir;
  }
  
  // we wrap the directory in slow stuff, so only run nightly
  @Override @Nightly
  public void testThreadSafety() throws Exception {
    super.testThreadSafety();
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
    out = dir.createOutput("bar", IOContext.DEFAULT);
    try {
      out.writeBytes(bytes, bytes.length);
      fail("should have failed on disk full");
    } catch (IOException e) {
      // expected
    }
    out.close();
    dir.close();
    
    // test copyBytes
    dir = newMockDirectory();
    dir.setMaxSizeInBytes(3);
    out = dir.createOutput("foo", IOContext.DEFAULT);
    out.copyBytes(new ByteArrayDataInput(bytes), bytes.length); // first copy should succeed
    // close() to ensure the written bytes are not buffered and counted
    // against the directory size
    out.close();
    out = dir.createOutput("bar", IOContext.DEFAULT);
    try {
      out.copyBytes(new ByteArrayDataInput(bytes), bytes.length);
      fail("should have failed on disk full");
    } catch (IOException e) {
      // expected
    }
    out.close();
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
}
