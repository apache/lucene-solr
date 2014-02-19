package org.apache.lucene.replicator;

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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.IOUtils;
import org.junit.Test;

public class NRTIndexRevisionTest extends ReplicatorTestCase {
  
  private DirectoryReader openReader(Directory dir, Revision rev) throws IOException {
    Map<String,List<RevisionFile>> sourceFiles = rev.getSourceFiles();
    String source = sourceFiles.keySet().iterator().next();
    String segmentsFile = null;
    for (RevisionFile rf : sourceFiles.values().iterator().next()) {
      if (rf.fileName.startsWith(IndexFileNames.SEGMENTS)) {
        segmentsFile = rf.fileName;
      }
    }
    assertNotNull(segmentsFile);
    SegmentInfos infos = new SegmentInfos();
    infos.read(dir, new InputStreamDataInput(rev.open(source, segmentsFile)));
    DirectoryReader reader = StandardDirectoryReader.open(dir, infos, null);
    return reader;
  }
  
  @Test
  public void testMixedCommit() throws Exception {
    // verify NRTIndexRevision works with commits too
    Directory dir = newDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, null);
    IndexWriter writer = new IndexWriter(dir, conf);
    // no changes yet
    Revision rev = new NRTIndexRevision(writer);
    DirectoryReader reader = openReader(dir, rev);
    assertEquals(0, reader.numDocs());
    reader.close();
    rev.release();
    
    writer.addDocument(new Document());
    rev = new NRTIndexRevision(writer);
    reader = openReader(dir, rev);
    assertEquals(1, reader.numDocs());
    reader.close();
    rev.release();
    
    writer.addDocument(new Document());
    writer.commit();
    rev = new NRTIndexRevision(writer);
    int numSegmentsFiles = 0;
    for (RevisionFile rf : rev.getSourceFiles().values().iterator().next()) {
      if (rf.fileName.startsWith("segments_")) {
        ++numSegmentsFiles;
      }
    }
    assertEquals(2, numSegmentsFiles); // we currently list both the NRT SIS and the committed one

    writer.forceMerge(1);
    reader = openReader(dir, rev);
    assertEquals(2, reader.numDocs());
    reader.close();
    rev.release();
    // even though _0 and _1 were merged into _2, there is a commit point that references them
    assertTrue(dir.fileExists("_2.si"));
    assertTrue(dir.fileExists("_0.si"));
    assertTrue(dir.fileExists("_1.si"));

    writer.commit();
    // now they are not referenced anymore
    assertFalse(dir.fileExists("_0.si"));
    assertFalse(dir.fileExists("_1.si"));
    
    IOUtils.close(writer, dir);
  }
  
  @Test
  public void testNoCommit() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, null);
    IndexWriter writer = new IndexWriter(dir, conf);
    // no changes yet
    NRTIndexRevision rev = new NRTIndexRevision(writer);
    assertEquals("nrt_1", rev.getVersion());
    assertEquals(1, rev.getSourceFiles().size());
    rev.release();
    writer.addDocument(new Document());
    rev = new NRTIndexRevision(writer);
    assertEquals("nrt_3", rev.getVersion());
    rev.release();
    IOUtils.close(writer, dir);
  }
  
  @Test
  public void testOpen() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, null);
    IndexWriter writer = new IndexWriter(dir, conf);
    try {
      writer.addDocument(new Document());
      Revision rev = new NRTIndexRevision(writer);
      DirectoryReader reader = openReader(dir, rev);
      assertEquals(1, reader.numDocs());
      reader.close();
    } finally {
      IOUtils.close(writer, dir);
    }
  }
  
  @Test
  public void testRevisionRelease() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, null);
    IndexWriter writer = new IndexWriter(dir, conf);
    try {
      writer.addDocument(new Document());
      new NRTIndexRevision(writer).release(); // trigger a flush
      writer.addDocument(new Document());
      Revision rev = new NRTIndexRevision(writer); // trigger another flush, but don't commit
      writer.forceMerge(1);
      // files should not be deleted after forceMerge
      assertTrue(dir.fileExists("_0.si"));
      assertTrue(dir.fileExists("_1.si"));
      rev.release();
      // now old segments should have been deleted
      assertTrue(dir.fileExists("_2.si"));
      assertFalse(dir.fileExists("_0.si"));
      assertFalse(dir.fileExists("_1.si"));
    } finally {
      IOUtils.close(writer, dir);
    }
  }

  @Test
  public void testSegmentsFileLast() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, null);
    IndexWriter writer = new IndexWriter(dir, conf);
    try {
      writer.addDocument(new Document());
      writer.commit();
      Revision rev = new NRTIndexRevision(writer);
      @SuppressWarnings("unchecked")
      Map<String, List<RevisionFile>> sourceFiles = rev.getSourceFiles();
      assertEquals(1, sourceFiles.size());
      List<RevisionFile> files = sourceFiles.values().iterator().next();
      String lastFile = files.get(files.size() - 1).fileName;
      assertTrue(lastFile.startsWith(IndexFileNames.SEGMENTS) && !lastFile.equals(IndexFileNames.SEGMENTS_GEN));
    } finally {
      IOUtils.close(writer, dir);
    }
  }
  
}
