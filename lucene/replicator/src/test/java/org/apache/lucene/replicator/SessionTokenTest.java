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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.junit.Test;

public class SessionTokenTest extends ReplicatorTestCase {
  
  @Test
  public void testSerialization() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(null);
    conf.setIndexDeletionPolicy(new SnapshotDeletionPolicy(conf.getIndexDeletionPolicy()));
    IndexWriter writer = new IndexWriter(dir, conf);
    writer.addDocument(new Document());
    writer.commit();
    Revision rev = new IndexRevision(writer);
    
    SessionToken session1 = new SessionToken("17", rev);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    session1.serialize(new DataOutputStream(baos));
    byte[] b = baos.toByteArray();
    SessionToken session2 = new SessionToken(new DataInputStream(new ByteArrayInputStream(b)));
    assertEquals(session1.id, session2.id);
    assertEquals(session1.version, session2.version);
    assertEquals(1, session2.sourceFiles.size());
    assertEquals(session1.sourceFiles.size(), session2.sourceFiles.size());
    assertEquals(session1.sourceFiles.keySet(), session2.sourceFiles.keySet());
    List<RevisionFile> files1 = session1.sourceFiles.values().iterator().next();
    List<RevisionFile> files2 = session2.sourceFiles.values().iterator().next();
    assertEquals(files1, files2);

    writer.close();
    IOUtils.close(dir);
  }
  
}
