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
package org.apache.lucene.replicator;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.IOUtils;
import org.junit.Test;

public class IndexRevisionTest extends ReplicatorTestCase {
  
  @Test
  public void testNoSnapshotDeletionPolicy() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(null);
    conf.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    IndexWriter writer = new IndexWriter(dir, conf);
    try {
      assertNotNull(new IndexRevision(writer));
      fail("should have failed when IndexDeletionPolicy is not Snapshot");
    } catch (IllegalArgumentException e) {
      // expected
    } finally {
      writer.close();
      IOUtils.close(dir);
    }
  }
  
  @Test
  public void testNoCommit() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(null);
    conf.setIndexDeletionPolicy(new SnapshotDeletionPolicy(conf.getIndexDeletionPolicy()));
    IndexWriter writer = new IndexWriter(dir, conf);
    try {
      assertNotNull(new IndexRevision(writer));
      fail("should have failed when there are no commits to snapshot");
    } catch (IllegalStateException e) {
      // expected
    } finally {
      writer.close();
      IOUtils.close(dir);
    }
  }
  
  @Test
  public void testRevisionRelease() throws Exception {
    Directory dir = newDirectory();
    // we look to see that certain files are deleted:
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir).setEnableVirusScanner(false);
    }
    IndexWriterConfig conf = new IndexWriterConfig(null);
    conf.setIndexDeletionPolicy(new SnapshotDeletionPolicy(conf.getIndexDeletionPolicy()));
    IndexWriter writer = new IndexWriter(dir, conf);
    try {
      writer.addDocument(new Document());
      writer.commit();
      Revision rev1 = new IndexRevision(writer);
      // releasing that revision should not delete the files
      rev1.release();
      assertTrue(slowFileExists(dir, IndexFileNames.SEGMENTS + "_1"));
      
      rev1 = new IndexRevision(writer); // create revision again, so the files are snapshotted
      writer.addDocument(new Document());
      writer.commit();
      assertNotNull(new IndexRevision(writer));
      rev1.release(); // this release should trigger the delete of segments_1
      assertFalse(slowFileExists(dir, IndexFileNames.SEGMENTS + "_1"));
    } finally {
      IOUtils.close(writer, dir);
    }
  }
  
  @Test
  public void testSegmentsFileLast() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(null);
    conf.setIndexDeletionPolicy(new SnapshotDeletionPolicy(conf.getIndexDeletionPolicy()));
    IndexWriter writer = new IndexWriter(dir, conf);
    try {
      writer.addDocument(new Document());
      writer.commit();
      Revision rev = new IndexRevision(writer);
      @SuppressWarnings("unchecked")
      Map<String, List<RevisionFile>> sourceFiles = rev.getSourceFiles();
      assertEquals(1, sourceFiles.size());
      List<RevisionFile> files = sourceFiles.values().iterator().next();
      String lastFile = files.get(files.size() - 1).fileName;
      assertTrue(lastFile.startsWith(IndexFileNames.SEGMENTS));
      writer.close();
    } finally {
      IOUtils.close(dir);
    }
  }
  
  @Test
  public void testOpen() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(null);
    conf.setIndexDeletionPolicy(new SnapshotDeletionPolicy(conf.getIndexDeletionPolicy()));
    IndexWriter writer = new IndexWriter(dir, conf);
    try {
      writer.addDocument(new Document());
      writer.commit();
      Revision rev = new IndexRevision(writer);
      @SuppressWarnings("unchecked")
      Map<String, List<RevisionFile>> sourceFiles = rev.getSourceFiles();
      String source = sourceFiles.keySet().iterator().next();
      for (RevisionFile file : sourceFiles.values().iterator().next()) {
        IndexInput src = dir.openInput(file.fileName, IOContext.READONCE);
        InputStream in = rev.open(source, file.fileName);
        assertEquals(src.length(), in.available());
        byte[] srcBytes = new byte[(int) src.length()];
        byte[] inBytes = new byte[(int) src.length()];
        int offset = 0;
        if (random().nextBoolean()) {
          int skip = random().nextInt(10);
          if (skip >= src.length()) {
            skip = 0;
          }
          in.skip(skip);
          src.seek(skip);
          offset = skip;
        }
        src.readBytes(srcBytes, offset, srcBytes.length - offset);
        in.read(inBytes, offset, inBytes.length - offset);
        assertArrayEquals(srcBytes, inBytes);
        IOUtils.close(src, in);
      }
      writer.close();
    } finally {
      IOUtils.close(dir);
    }
  }
  
}
