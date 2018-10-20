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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LocalReplicatorTest extends ReplicatorTestCase {
  
  private static final String VERSION_ID = "version";
  
  private LocalReplicator replicator;
  private Directory sourceDir;
  private IndexWriter sourceWriter;
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    sourceDir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(null);
    conf.setIndexDeletionPolicy(new SnapshotDeletionPolicy(conf.getIndexDeletionPolicy()));
    sourceWriter = new IndexWriter(sourceDir, conf);
    replicator = new LocalReplicator();
  }
  
  @After
  @Override
  public void tearDown() throws Exception {
    sourceWriter.close();
    IOUtils.close(replicator, sourceDir);
    super.tearDown();
  }
  
  private Revision createRevision(final int id) throws IOException {
    sourceWriter.addDocument(new Document());
    sourceWriter.setLiveCommitData(new HashMap<String, String>() {{
      put(VERSION_ID, Integer.toString(id, 16));
    }}.entrySet());
    sourceWriter.commit();
    return new IndexRevision(sourceWriter);
  }
  
  @Test
  public void testCheckForUpdateNoRevisions() throws Exception {
    assertNull(replicator.checkForUpdate(null));
  }
  
  @Test
  public void testObtainFileAlreadyClosed() throws IOException {
    replicator.publish(createRevision(1));
    SessionToken res = replicator.checkForUpdate(null);
    assertNotNull(res);
    assertEquals(1, res.sourceFiles.size());
    Entry<String,List<RevisionFile>> entry = res.sourceFiles.entrySet().iterator().next();
    replicator.close();
    expectThrows(AlreadyClosedException.class, () -> {
      replicator.obtainFile(res.id, entry.getKey(), entry.getValue().get(0).fileName);
    });
  }
  
  @Test
  public void testPublishAlreadyClosed() throws IOException {
    replicator.close();
    expectThrows(AlreadyClosedException.class, () -> {
      replicator.publish(createRevision(2));
    });
  }
  
  @Test
  public void testUpdateAlreadyClosed() throws IOException {
    replicator.close();
    expectThrows(AlreadyClosedException.class, () -> {
      replicator.checkForUpdate(null);
    });
  }
  
  @Test
  public void testPublishSameRevision() throws IOException {
    Revision rev = createRevision(1);
    replicator.publish(rev);
    SessionToken res = replicator.checkForUpdate(null);
    assertNotNull(res);
    assertEquals(rev.getVersion(), res.version);
    replicator.release(res.id);
    replicator.publish(new IndexRevision(sourceWriter));
    res = replicator.checkForUpdate(res.version);
    assertNull(res);
      
    // now make sure that publishing same revision doesn't leave revisions
    // "locked", i.e. that replicator releases revisions even when they are not
    // kept
    replicator.publish(createRevision(2));
    assertEquals(1, DirectoryReader.listCommits(sourceDir).size());
  }
  
  @Test
  public void testPublishOlderRev() throws IOException {
    replicator.publish(createRevision(1));
    Revision old = new IndexRevision(sourceWriter);
    replicator.publish(createRevision(2));
    // should fail to publish an older revision
    expectThrows(IllegalArgumentException.class, () -> {
      replicator.publish(old);
    });
    assertEquals(1, DirectoryReader.listCommits(sourceDir).size());
  }
  
  @Test
  public void testObtainMissingFile() throws IOException {
    replicator.publish(createRevision(1));
    SessionToken res = replicator.checkForUpdate(null);
    try {
      replicator.obtainFile(res.id, res.sourceFiles.keySet().iterator().next(), "madeUpFile");
      fail("should have failed obtaining an unrecognized file");
    } catch (FileNotFoundException | NoSuchFileException e) {
      // expected
    }
  }
  
  @Test
  public void testSessionExpiration() throws IOException, InterruptedException {
    replicator.publish(createRevision(1));
    SessionToken session = replicator.checkForUpdate(null);
    replicator.setExpirationThreshold(5); // expire quickly
    Thread.sleep(50); // sufficient for expiration
    // should fail to obtain a file for an expired session
    expectThrows(SessionExpiredException.class, () -> {
      replicator.obtainFile(session.id, session.sourceFiles.keySet().iterator().next(), session.sourceFiles.values().iterator().next().get(0).fileName);
    });
  }
  
  @Test
  public void testUpdateToLatest() throws IOException {
    replicator.publish(createRevision(1));
    Revision rev = createRevision(2);
    replicator.publish(rev);
    SessionToken res = replicator.checkForUpdate(null);
    assertNotNull(res);
    assertEquals(0, rev.compareTo(res.version));
  }
  
  @Test
  public void testRevisionRelease() throws Exception {
    replicator.publish(createRevision(1));
    assertTrue(slowFileExists(sourceDir, IndexFileNames.SEGMENTS + "_1"));
    replicator.publish(createRevision(2));
    // now the files of revision 1 can be deleted
    assertTrue(slowFileExists(sourceDir, IndexFileNames.SEGMENTS + "_2"));
    assertFalse("segments_1 should not be found in index directory after revision is released", slowFileExists(sourceDir, IndexFileNames.SEGMENTS + "_1"));
  }
  
}
