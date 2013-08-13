package org.apache.lucene.replicator.http;

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
import java.io.IOException;
import java.util.Collections;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.replicator.IndexReplicationHandler;
import org.apache.lucene.replicator.IndexRevision;
import org.apache.lucene.replicator.LocalReplicator;
import org.apache.lucene.replicator.PerSessionDirectoryFactory;
import org.apache.lucene.replicator.ReplicationClient;
import org.apache.lucene.replicator.Replicator;
import org.apache.lucene.replicator.ReplicatorTestCase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util._TestUtil;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Before;
import org.junit.Test;

public class HttpReplicatorTest extends ReplicatorTestCase {
  
  private File clientWorkDir;
  private Replicator serverReplicator;
  private IndexWriter writer;
  private DirectoryReader reader;
  private Server server;
  private int port;
  private String host;
  private Directory serverIndexDir, handlerIndexDir;
  
  private void startServer() throws Exception {
    ServletHandler replicationHandler = new ServletHandler();
    ReplicationService service = new ReplicationService(Collections.singletonMap("s1", serverReplicator));
    ServletHolder servlet = new ServletHolder(new ReplicationServlet(service));
    replicationHandler.addServletWithMapping(servlet, ReplicationService.REPLICATION_CONTEXT + "/*");
    server = newHttpServer(replicationHandler);
    port = serverPort(server);
    host = serverHost(server);
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("org.eclipse.jetty.LEVEL", "DEBUG"); // sets stderr logging to DEBUG level
    clientWorkDir = _TestUtil.getTempDir("httpReplicatorTest");
    handlerIndexDir = newDirectory();
    serverIndexDir = newDirectory();
    serverReplicator = new LocalReplicator();
    startServer();
    
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, null);
    conf.setIndexDeletionPolicy(new SnapshotDeletionPolicy(conf.getIndexDeletionPolicy()));
    writer = new IndexWriter(serverIndexDir, conf);
    reader = DirectoryReader.open(writer, false);
  }
  
  @Override
  public void tearDown() throws Exception {
    stopHttpServer(server);
    IOUtils.close(reader, writer, handlerIndexDir, serverIndexDir);
    System.clearProperty("org.eclipse.jetty.LEVEL");
    super.tearDown();
  }
  
  private void publishRevision(int id) throws IOException {
    Document doc = new Document();
    writer.addDocument(doc);
    writer.setCommitData(Collections.singletonMap("ID", Integer.toString(id, 16)));
    writer.commit();
    serverReplicator.publish(new IndexRevision(writer));
  }
  
  private void reopenReader() throws IOException {
    DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
    assertNotNull(newReader);
    reader.close();
    reader = newReader;
  }
  
  @Test
  public void testBasic() throws Exception {
    Replicator replicator = new HttpReplicator(host, port, ReplicationService.REPLICATION_CONTEXT + "/s1", 
        getClientConnectionManager());
    ReplicationClient client = new ReplicationClient(replicator, new IndexReplicationHandler(handlerIndexDir, null), 
        new PerSessionDirectoryFactory(clientWorkDir));
    
    publishRevision(1);
    client.updateNow();
    reopenReader();
    assertEquals(1, Integer.parseInt(reader.getIndexCommit().getUserData().get("ID"), 16));
    
    publishRevision(2);
    client.updateNow();
    reopenReader();
    assertEquals(2, Integer.parseInt(reader.getIndexCommit().getUserData().get("ID"), 16));
  }
  
}
