package org.apache.lucene.store;

/**
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
import java.io.IOException;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Tests MMapDirectory's MultiMMapIndexInput
 * <p>
 * Because Java's ByteBuffer uses an int to address the
 * values, it's necessary to access a file >
 * Integer.MAX_VALUE in size using multiple byte buffers.
 */
public class TestMultiMMap extends LuceneTestCase {
  File workDir;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    assumeTrue("test requires a jre that supports unmapping", MMapDirectory.UNMAP_SUPPORTED);
    workDir = _TestUtil.getTempDir("TestMultiMMap");
    workDir.mkdirs();
  }
  
  public void testCloneSafety() throws Exception {
    MMapDirectory mmapDir = new MMapDirectory(_TestUtil.getTempDir("testCloneSafety"));
    IndexOutput io = mmapDir.createOutput("bytes", newIOContext(random()));
    io.writeVInt(5);
    io.close();
    IndexInput one = mmapDir.openInput("bytes", IOContext.DEFAULT);
    IndexInput two = (IndexInput) one.clone();
    IndexInput three = (IndexInput) two.clone(); // clone of clone
    one.close();
    try {
      one.readVInt();
      fail("Must throw AlreadyClosedException");
    } catch (AlreadyClosedException ignore) {
      // pass
    }
    try {
      two.readVInt();
      fail("Must throw AlreadyClosedException");
    } catch (AlreadyClosedException ignore) {
      // pass
    }
    try {
      three.readVInt();
      fail("Must throw AlreadyClosedExveption");
    } catch (AlreadyClosedException ignore) {
      // pass
    }
  }

  public void testSeekZero() throws Exception {
    for (int i = 0; i < 31; i++) {
      MMapDirectory mmapDir = new MMapDirectory(_TestUtil.getTempDir("testSeekZero"));
      mmapDir.setMaxChunkSize(1<<i);
      IndexOutput io = mmapDir.createOutput("zeroBytes", newIOContext(random()));
      io.close();
      IndexInput ii = mmapDir.openInput("zeroBytes", newIOContext(random()));
      ii.seek(0L);
      ii.close();
      mmapDir.close();
    }
  }
  
  public void testSeekEnd() throws Exception {
    for (int i = 0; i < 17; i++) {
      MMapDirectory mmapDir = new MMapDirectory(_TestUtil.getTempDir("testSeekEnd"));
      mmapDir.setMaxChunkSize(1<<i);
      IndexOutput io = mmapDir.createOutput("bytes", newIOContext(random()));
      byte bytes[] = new byte[1<<i];
      random().nextBytes(bytes);
      io.writeBytes(bytes, bytes.length);
      io.close();
      IndexInput ii = mmapDir.openInput("bytes", newIOContext(random()));
      byte actual[] = new byte[1<<i];
      ii.readBytes(actual, 0, actual.length);
      assertEquals(new BytesRef(bytes), new BytesRef(actual));
      ii.seek(1<<i);
      ii.close();
      mmapDir.close();
    }
  }
  
  public void testSeeking() throws Exception {
    for (int i = 0; i < 10; i++) {
      MMapDirectory mmapDir = new MMapDirectory(_TestUtil.getTempDir("testSeeking"));
      mmapDir.setMaxChunkSize(1<<i);
      IndexOutput io = mmapDir.createOutput("bytes", newIOContext(random()));
      byte bytes[] = new byte[1<<(i+1)]; // make sure we switch buffers
      random().nextBytes(bytes);
      io.writeBytes(bytes, bytes.length);
      io.close();
      IndexInput ii = mmapDir.openInput("bytes", newIOContext(random()));
      byte actual[] = new byte[1<<(i+1)]; // first read all bytes
      ii.readBytes(actual, 0, actual.length);
      assertEquals(new BytesRef(bytes), new BytesRef(actual));
      for (int sliceStart = 0; sliceStart < bytes.length; sliceStart++) {
        for (int sliceLength = 0; sliceLength < bytes.length - sliceStart; sliceLength++) {
          byte slice[] = new byte[sliceLength];
          ii.seek(sliceStart);
          ii.readBytes(slice, 0, slice.length);
          assertEquals(new BytesRef(bytes, sliceStart, sliceLength), new BytesRef(slice));
        }
      }
      ii.close();
      mmapDir.close();
    }
  }
  
  public void testRandomChunkSizes() throws Exception {
    int num = atLeast(10);
    for (int i = 0; i < num; i++)
      assertChunking(random(), _TestUtil.nextInt(random(), 20, 100));
  }
  
  private void assertChunking(Random random, int chunkSize) throws Exception {
    File path = _TestUtil.createTempFile("mmap" + chunkSize, "tmp", workDir);
    path.delete();
    path.mkdirs();
    MMapDirectory mmapDir = new MMapDirectory(path);
    mmapDir.setMaxChunkSize(chunkSize);
    // we will map a lot, try to turn on the unmap hack
    if (MMapDirectory.UNMAP_SUPPORTED)
      mmapDir.setUseUnmap(true);
    MockDirectoryWrapper dir = new MockDirectoryWrapper(random, mmapDir);
    RandomIndexWriter writer = new RandomIndexWriter(random, dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy()));
    Document doc = new Document();
    Field docid = newField("docid", "0", StringField.TYPE_STORED);
    Field junk = newField("junk", "", StringField.TYPE_STORED);
    doc.add(docid);
    doc.add(junk);
    
    int numDocs = 100;
    for (int i = 0; i < numDocs; i++) {
      docid.setStringValue("" + i);
      junk.setStringValue(_TestUtil.randomUnicodeString(random));
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();
    writer.close();
    
    int numAsserts = atLeast(100);
    for (int i = 0; i < numAsserts; i++) {
      int docID = random.nextInt(numDocs);
      assertEquals("" + docID, reader.document(docID).get("docid"));
    }
    reader.close();
    dir.close();
  }
}
