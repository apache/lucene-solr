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

package org.apache.lucene.replicator.nrt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.TestRuleIgnoreTestSuites;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.SeedUtils;

// MockRandom's .sd file has no index header/footer:
@SuppressCodecs({"MockRandom", "Direct", "SimpleText"})
@SuppressSysoutChecks(bugUrl = "Stuff gets printed, important stuff for debugging a failure")
public class TestNRTReplication extends LuceneTestCase {

  /** cwd where we start each child (server) node */
  private Path childTempDir;

  final AtomicLong nodeStartCounter = new AtomicLong();
  private long nextPrimaryGen;
  private long lastPrimaryGen;
  LineFileDocs docs;

  /** Launches a child "server" (separate JVM), which is either primary or replica node */
  @SuppressForbidden(reason = "ProcessBuilder requires java.io.File for CWD")
  private NodeProcess startNode(int primaryTCPPort, final int id, Path indexPath, long forcePrimaryVersion, boolean willCrash) throws IOException {
    List<String> cmd = new ArrayList<>();

    cmd.add(System.getProperty("java.home") 
        + System.getProperty("file.separator")
        + "bin"
        + System.getProperty("file.separator")
        + "java");
    cmd.add("-Xmx512m");

    long myPrimaryGen;
    if (primaryTCPPort != -1) {
      // I am a replica
      cmd.add("-Dtests.nrtreplication.primaryTCPPort=" + primaryTCPPort);
      myPrimaryGen = lastPrimaryGen;
    } else {
      myPrimaryGen = nextPrimaryGen++;
      lastPrimaryGen = myPrimaryGen;
    }
    cmd.add("-Dtests.nrtreplication.primaryGen=" + myPrimaryGen);
    cmd.add("-Dtests.nrtreplication.closeorcrash=false");

    cmd.add("-Dtests.nrtreplication.node=true");
    cmd.add("-Dtests.nrtreplication.nodeid=" + id);
    cmd.add("-Dtests.nrtreplication.startNS=" + Node.globalStartNS);
    cmd.add("-Dtests.nrtreplication.indexpath=" + indexPath);
    cmd.add("-Dtests.nrtreplication.checkonclose=true");

    if (primaryTCPPort == -1) {
      // We are the primary node
      cmd.add("-Dtests.nrtreplication.isPrimary=true");
      cmd.add("-Dtests.nrtreplication.forcePrimaryVersion=" + forcePrimaryVersion);
    }

    // Mark as running nested.
    cmd.add("-D" + TestRuleIgnoreTestSuites.PROPERTY_RUN_NESTED + "=true");

    // Mixin our own counter because this is called from a fresh thread which means the seed
    // otherwise isn't changing each time we spawn a
    // new node:
    long seed = random().nextLong() * nodeStartCounter.incrementAndGet();
    cmd.add("-Dtests.seed=" + SeedUtils.formatSeed(seed));
    cmd.add("-ea");
    cmd.add("-cp");
    cmd.add(System.getProperty("java.class.path"));
    cmd.add("org.junit.runner.JUnitCore");
    cmd.add(TestSimpleServer.class.getName());

    message("child process command: " + cmd);
    ProcessBuilder pb = new ProcessBuilder(cmd);
    pb.redirectErrorStream(true);

    // Important, so that the scary looking hs_err_<pid>.log appear under our test temp dir:
    pb.directory(childTempDir.toFile());

    Process p = pb.start();

    BufferedReader r;
    try {
      r = new BufferedReader(new InputStreamReader(p.getInputStream(), IOUtils.UTF_8));
    } catch (UnsupportedEncodingException uee) {
      throw new RuntimeException(uee);
    }

    int tcpPort = -1;
    long initCommitVersion = -1;
    long initInfosVersion = -1;
    Pattern logTimeStart = Pattern.compile("^[0-9\\.]+s .*");
    boolean sawExistingSegmentsFile = false;

    while (true) {
      String l = r.readLine();
      if (l == null) {
        message("top: node=" + id + " failed to start");
        try {
          p.waitFor();
        } catch (InterruptedException ie) {
          throw new RuntimeException(ie);
        }
        message("exit value=" + p.exitValue());
        message("top: now fail test replica R" + id + " failed to start");
        throw new RuntimeException("replica R" + id + " failed to start");
      }

      if (logTimeStart.matcher(l).matches()) {
        // Already a well-formed log output:
        System.out.println(l);
      } else {
        message(l);
      }

      if (l.startsWith("PORT: ")) {
        tcpPort = Integer.parseInt(l.substring(6).trim());
      } else if (l.startsWith("COMMIT VERSION: ")) {
        initCommitVersion = Integer.parseInt(l.substring(16).trim());
      } else if (l.startsWith("INFOS VERSION: ")) {
        initInfosVersion = Integer.parseInt(l.substring(15).trim());
      } else if (l.contains("will crash after")) {
        willCrash = true;
      } else if (l.startsWith("NODE STARTED")) {
        break;
      } else if (l.contains("replica cannot start: existing segments file=")) {
        sawExistingSegmentsFile = true;
      }
    }

    final boolean finalWillCrash = willCrash;

    // Baby sits the child process, pulling its stdout and printing to our stdout:
    AtomicBoolean nodeClosing = new AtomicBoolean();
    Thread pumper = ThreadPumper.start(
                                       new Runnable() {
                                         @Override
                                         public void run() {
                                           message("now wait for process " + p);
                                           try {
                                             p.waitFor();
                                           } catch (Throwable t) {
                                             throw new RuntimeException(t);
                                           }

                                           message("done wait for process " + p);
                                           int exitValue = p.exitValue();
                                           message("exit value=" + exitValue + " willCrash=" + finalWillCrash);
                                           if (exitValue != 0 && finalWillCrash == false) {
                                             // should fail test
                                             throw new RuntimeException("node " + id + " process had unexpected non-zero exit status=" + exitValue);
                                           }
                                         }
                                       }, r, System.out, null, nodeClosing);
    pumper.setName("pump" + id);

    message("top: node=" + id + " started at tcpPort=" + tcpPort + " initCommitVersion=" + initCommitVersion + " initInfosVersion=" + initInfosVersion);
    return new NodeProcess(p, id, tcpPort, pumper, primaryTCPPort == -1, initCommitVersion, initInfosVersion, nodeClosing);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Node.globalStartNS = System.nanoTime();
    childTempDir = createTempDir("child");
    docs = new LineFileDocs(random());
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    docs.close();
  }

  @Nightly
  public void testReplicateDeleteAllDocuments() throws Exception {

    Path primaryPath = createTempDir("primary");
    NodeProcess primary = startNode(-1, 0, primaryPath, -1, false);

    Path replicaPath = createTempDir("replica");
    NodeProcess replica = startNode(primary.tcpPort, 1, replicaPath, -1, false);

    // Tell primary current replicas:
    sendReplicasToPrimary(primary, replica);

    // Index 10 docs into primary:
    LineFileDocs docs = new LineFileDocs(random());
    Connection primaryC = new Connection(primary.tcpPort);
    primaryC.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
    for(int i=0;i<10;i++) {
      Document doc = docs.nextDoc();
      primary.addOrUpdateDocument(primaryC, doc, false);
    }

    // Nothing in replica index yet
    assertVersionAndHits(replica, 0, 0);

    // Refresh primary, which also pushes to replica:
    long primaryVersion1 = primary.flush(0);
    assertTrue(primaryVersion1 > 0);

    // Wait for replica to show the change
    waitForVersionAndHits(replica, primaryVersion1, 10);

    // Delete all docs from primary
    if (random().nextBoolean()) {
      // Inefficiently:
      for(int id=0;id<10;id++) {
        primary.deleteDocument(primaryC, Integer.toString(id));
      }
    } else {
      // Efficiently:
      primary.deleteAllDocuments(primaryC);
    }

    // Replica still shows 10 docs:
    assertVersionAndHits(replica, primaryVersion1, 10);
    
    // Refresh primary, which also pushes to replica:
    long primaryVersion2 = primary.flush(0);
    assertTrue(primaryVersion2 > primaryVersion1);

    // Wait for replica to show the change
    waitForVersionAndHits(replica, primaryVersion2, 0);

    // Index 10 docs again:
    for(int i=0;i<10;i++) {
      Document doc = docs.nextDoc();
      primary.addOrUpdateDocument(primaryC, doc, false);
    }

    // Refresh primary, which also pushes to replica:
    long primaryVersion3 = primary.flush(0);
    assertTrue(primaryVersion3 > primaryVersion2);

    // Wait for replica to show the change
    waitForVersionAndHits(replica, primaryVersion3, 10);

    primaryC.close();
    docs.close();
    replica.close();
    primary.close();
  }

  @Nightly
  public void testReplicateForceMerge() throws Exception {

    Path primaryPath = createTempDir("primary");
    NodeProcess primary = startNode(-1, 0, primaryPath, -1, false);

    Path replicaPath = createTempDir("replica");
    NodeProcess replica = startNode(primary.tcpPort, 1, replicaPath, -1, false);

    sendReplicasToPrimary(primary, replica);

    // Index 10 docs into primary:
    LineFileDocs docs = new LineFileDocs(random());
    Connection primaryC = new Connection(primary.tcpPort);
    primaryC.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
    for(int i=0;i<10;i++) {
      Document doc = docs.nextDoc();
      primary.addOrUpdateDocument(primaryC, doc, false);
    }

    // Refresh primary, which also pushes to replica:
    long primaryVersion1 = primary.flush(0);
    assertTrue(primaryVersion1 > 0);

    // Index 10 more docs into primary:
    for(int i=0;i<10;i++) {
      Document doc = docs.nextDoc();
      primary.addOrUpdateDocument(primaryC, doc, false);
    }

    // Refresh primary, which also pushes to replica:
    long primaryVersion2 = primary.flush(0);
    assertTrue(primaryVersion2 > primaryVersion1);

    primary.forceMerge(primaryC);

    // Refresh primary, which also pushes to replica:
    long primaryVersion3 = primary.flush(0);
    assertTrue(primaryVersion3 > primaryVersion2);

    // Wait for replica to show the change
    waitForVersionAndHits(replica, primaryVersion3, 20);

    primaryC.close();
    docs.close();
    replica.close();
    primary.close();
  }

  // Start up, index 10 docs, replicate, but crash and restart the replica without committing it:
  @Nightly
  public void testReplicaCrashNoCommit() throws Exception {

    Path primaryPath = createTempDir("primary");
    NodeProcess primary = startNode(-1, 0, primaryPath, -1, false);

    Path replicaPath = createTempDir("replica");
    NodeProcess replica = startNode(primary.tcpPort, 1, replicaPath, -1, true);

    sendReplicasToPrimary(primary, replica);

    // Index 10 docs into primary:
    LineFileDocs docs = new LineFileDocs(random());
    try (Connection c = new Connection(primary.tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
      for(int i=0;i<10;i++) {
        Document doc = docs.nextDoc();
        primary.addOrUpdateDocument(c, doc, false);
      }
    }

    // Refresh primary, which also pushes to replica:
    long primaryVersion1 = primary.flush(0);
    assertTrue(primaryVersion1 > 0);

    // Wait for replica to sync up:
    waitForVersionAndHits(replica, primaryVersion1, 10);

    // Crash replica:
    replica.crash();

    // Restart replica:
    replica = startNode(primary.tcpPort, 1, replicaPath, -1, false);

    // On startup the replica searches the last commit (empty here):
    assertVersionAndHits(replica, 0, 0);

    // Ask replica to sync:
    replica.newNRTPoint(primaryVersion1, 0, primary.tcpPort);
    waitForVersionAndHits(replica, primaryVersion1, 10);
    docs.close();
    replica.close();
    primary.close();
  }

  // Start up, index 10 docs, replicate, commit, crash and restart the replica
  @Nightly
  public void testReplicaCrashWithCommit() throws Exception {

    Path primaryPath = createTempDir("primary");
    NodeProcess primary = startNode(-1, 0, primaryPath, -1, false);

    Path replicaPath = createTempDir("replica");
    NodeProcess replica = startNode(primary.tcpPort, 1, replicaPath, -1, true);

    sendReplicasToPrimary(primary, replica);

    // Index 10 docs into primary:
    LineFileDocs docs = new LineFileDocs(random());
    try (Connection c = new Connection(primary.tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
      for(int i=0;i<10;i++) {
        Document doc = docs.nextDoc();
        primary.addOrUpdateDocument(c, doc, false);
      }
    }

    // Refresh primary, which also pushes to replica:
    long primaryVersion1 = primary.flush(0);
    assertTrue(primaryVersion1 > 0);

    // Wait for replica to sync up:
    waitForVersionAndHits(replica, primaryVersion1, 10);

    // Commit and crash replica:
    replica.commit();
    replica.crash();

    // Restart replica:
    replica = startNode(primary.tcpPort, 1, replicaPath, -1, false);

    // On startup the replica searches the last commit:
    assertVersionAndHits(replica, primaryVersion1, 10);
    docs.close();
    replica.close();
    primary.close();
  }

  // Start up, index 10 docs, replicate, commit, crash, index more docs, replicate, then restart the replica
  @Nightly
  public void testIndexingWhileReplicaIsDown() throws Exception {

    Path primaryPath = createTempDir("primary");
    NodeProcess primary = startNode(-1, 0, primaryPath, -1, false);

    Path replicaPath = createTempDir("replica");
    NodeProcess replica = startNode(primary.tcpPort, 1, replicaPath, -1, true);

    sendReplicasToPrimary(primary, replica);

    // Index 10 docs into primary:
    LineFileDocs docs = new LineFileDocs(random());
    try (Connection c = new Connection(primary.tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
      for(int i=0;i<10;i++) {
        Document doc = docs.nextDoc();
        primary.addOrUpdateDocument(c, doc, false);
      }
    }

    // Refresh primary, which also pushes to replica:
    long primaryVersion1 = primary.flush(0);
    assertTrue(primaryVersion1 > 0);

    // Wait for replica to sync up:
    waitForVersionAndHits(replica, primaryVersion1, 10);

    // Commit and crash replica:
    replica.commit();
    replica.crash();

    sendReplicasToPrimary(primary);

    // Index 10 more docs, while replica is down
    try (Connection c = new Connection(primary.tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
      for(int i=0;i<10;i++) {
        Document doc = docs.nextDoc();
        primary.addOrUpdateDocument(c, doc, false);
      }
    }

    // And flush:
    long primaryVersion2 = primary.flush(0);
    assertTrue(primaryVersion2 > primaryVersion1);

    // Now restart replica:
    replica = startNode(primary.tcpPort, 1, replicaPath, -1, false);

    sendReplicasToPrimary(primary, replica);

    // On startup the replica still searches its last commit:
    assertVersionAndHits(replica, primaryVersion1, 10);

    // Now ask replica to sync:
    replica.newNRTPoint(primaryVersion2, 0, primary.tcpPort);

    waitForVersionAndHits(replica, primaryVersion2, 20);
    docs.close();
    replica.close();
    primary.close();
  }
 
  // Crash primary and promote a replica
  @Nightly
  public void testCrashPrimary1() throws Exception {

    Path path1 = createTempDir("1");
    NodeProcess primary = startNode(-1, 0, path1, -1, true);

    Path path2 = createTempDir("2");
    NodeProcess replica = startNode(primary.tcpPort, 1, path2, -1, true);

    sendReplicasToPrimary(primary, replica);

    // Index 10 docs into primary:
    LineFileDocs docs = new LineFileDocs(random());
    try (Connection c = new Connection(primary.tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
      for(int i=0;i<10;i++) {
        Document doc = docs.nextDoc();
        primary.addOrUpdateDocument(c, doc, false);
      }
    }

    // Refresh primary, which also pushes to replica:
    long primaryVersion1 = primary.flush(0);
    assertTrue(primaryVersion1 > 0);

    // Wait for replica to sync up:
    waitForVersionAndHits(replica, primaryVersion1, 10);

    docs.close();

    // Crash primary:
    primary.crash();

    // Promote replica:
    replica.commit();
    replica.close();
    
    primary = startNode(-1, 1, path2, -1, false);

    // Should still see 10 docs:
    assertVersionAndHits(primary, primaryVersion1, 10);

    primary.close();
  }

  // Crash primary and then restart it
  @Nightly
  public void testCrashPrimary2() throws Exception {

    Path path1 = createTempDir("1");
    NodeProcess primary = startNode(-1, 0, path1, -1, true);

    Path path2 = createTempDir("2");
    NodeProcess replica = startNode(primary.tcpPort, 1, path2, -1, true);

    sendReplicasToPrimary(primary, replica);

    // Index 10 docs into primary:
    LineFileDocs docs = new LineFileDocs(random());
    try (Connection c = new Connection(primary.tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
      for(int i=0;i<10;i++) {
        Document doc = docs.nextDoc();
        primary.addOrUpdateDocument(c, doc, false);
      }
    }

    // Refresh primary, which also pushes to replica:
    long primaryVersion1 = primary.flush(0);
    assertTrue(primaryVersion1 > 0);

    // Wait for replica to sync up:
    waitForVersionAndHits(replica, primaryVersion1, 10);

    primary.commit();

    // Index 10 docs, but crash before replicating or committing:
    try (Connection c = new Connection(primary.tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
      for(int i=0;i<10;i++) {
        Document doc = docs.nextDoc();
        primary.addOrUpdateDocument(c, doc, false);
      }
    }

    // Crash primary:
    primary.crash();

    // Restart it:
    primary = startNode(-1, 0, path1, -1, true);

    sendReplicasToPrimary(primary, replica);

    // Index 10 more docs
    try (Connection c = new Connection(primary.tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
      for(int i=0;i<10;i++) {
        Document doc = docs.nextDoc();
        primary.addOrUpdateDocument(c, doc, false);
      }
    }

    long primaryVersion2 = primary.flush(0);
    assertTrue(primaryVersion2 > primaryVersion1);

    // Wait for replica to sync up:
    waitForVersionAndHits(replica, primaryVersion2, 20);

    docs.close();
    primary.close();
    replica.close();
  }

  // Crash primary and then restart it, while a replica node is down, then bring replica node back up and make sure it properly "unforks" itself
  @Nightly
  public void testCrashPrimary3() throws Exception {

    Path path1 = createTempDir("1");
    NodeProcess primary = startNode(-1, 0, path1, -1, true);

    Path path2 = createTempDir("2");
    NodeProcess replica = startNode(primary.tcpPort, 1, path2, -1, true);

    sendReplicasToPrimary(primary, replica);

    // Index 10 docs into primary:
    try (Connection c = new Connection(primary.tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
      for(int i=0;i<10;i++) {
        Document doc = docs.nextDoc();
        primary.addOrUpdateDocument(c, doc, false);
      }
    }

    // Refresh primary, which also pushes to replica:
    long primaryVersion1 = primary.flush(0);
    assertTrue(primaryVersion1 > 0);

    // Wait for replica to sync up:
    waitForVersionAndHits(replica, primaryVersion1, 10);

    replica.commit();

    replica.close();
    primary.crash();

    // At this point replica is "in the future": it has 10 docs committed, but the primary crashed before committing so it has 0 docs

    // Restart primary:
    primary = startNode(-1, 0, path1, -1, true);

    // Index 20 docs into primary:
    try (Connection c = new Connection(primary.tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
      for(int i=0;i<20;i++) {
        Document doc = docs.nextDoc();
        primary.addOrUpdateDocument(c, doc, false);
      }
    }

    // Flush primary, but there are no replicas to sync to:
    long primaryVersion2 = primary.flush(0);

    // Now restart replica, which on init should detect on a "lost branch" because its 10 docs that were committed came from a different
    // primary node:
    replica = startNode(primary.tcpPort, 1, path2, -1, true);

    assertVersionAndHits(replica, primaryVersion2, 20);

    primary.close();
    replica.close();
  }

  @Nightly
  public void testCrashPrimaryWhileCopying() throws Exception {

    Path path1 = createTempDir("1");
    NodeProcess primary = startNode(-1, 0, path1, -1, true);

    Path path2 = createTempDir("2");
    NodeProcess replica = startNode(primary.tcpPort, 1, path2, -1, true);

    sendReplicasToPrimary(primary, replica);

    // Index 100 docs into primary:
    LineFileDocs docs = new LineFileDocs(random());
    try (Connection c = new Connection(primary.tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
      for(int i=0;i<100;i++) {
        Document doc = docs.nextDoc();
        primary.addOrUpdateDocument(c, doc, false);
      }
    }

    // Refresh primary, which also pushes (async) to replica:
    long primaryVersion1 = primary.flush(0);
    assertTrue(primaryVersion1 > 0);

    Thread.sleep(TestUtil.nextInt(random(), 1, 30));

    // Crash primary, likely/hopefully while replica is still copying
    primary.crash();

    // Could see either 100 docs (replica finished before crash) or 0 docs:
    try (Connection c = new Connection(replica.tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_SEARCH_ALL);
      c.flush();
      long version = c.in.readVLong();
      int hitCount = c.in.readVInt();
      if (version == 0) {
        assertEquals(0, hitCount);
      } else {
        assertEquals(primaryVersion1, version);
        assertEquals(100, hitCount);
      }
    }
    docs.close();
    primary.close();
    replica.close();
  }

  private void assertWriteLockHeld(Path path) throws Exception {
    try (FSDirectory dir = FSDirectory.open(path)) {
      expectThrows(LockObtainFailedException.class, () -> {dir.obtainLock(IndexWriter.WRITE_LOCK_NAME);});
    }
  }

  public void testCrashReplica() throws Exception {

    Path path1 = createTempDir("1");
    NodeProcess primary = startNode(-1, 0, path1, -1, true);

    Path path2 = createTempDir("2");
    NodeProcess replica = startNode(primary.tcpPort, 1, path2, -1, true);

    assertWriteLockHeld(path2);

    sendReplicasToPrimary(primary, replica);

    // Index 10 docs into primary:
    LineFileDocs docs = new LineFileDocs(random());
    try (Connection c = new Connection(primary.tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
      for(int i=0;i<10;i++) {
        Document doc = docs.nextDoc();
        primary.addOrUpdateDocument(c, doc, false);
      }
    }

    // Refresh primary, which also pushes to replica:
    long primaryVersion1 = primary.flush(0);
    assertTrue(primaryVersion1 > 0);

    // Wait for replica to sync up:
    waitForVersionAndHits(replica, primaryVersion1, 10);

    // Crash replica
    replica.crash();

    sendReplicasToPrimary(primary);

    // Lots of new flushes while replica is down:
    long primaryVersion2 = 0;
    for(int iter=0;iter<10;iter++) {
      // Index 10 docs into primary:
      try (Connection c = new Connection(primary.tcpPort)) {
        c.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
        for(int i=0;i<10;i++) {
          Document doc = docs.nextDoc();
          primary.addOrUpdateDocument(c, doc, false);
        }
      }
      primaryVersion2 = primary.flush(0);
    }

    // Start up replica again:
    replica = startNode(primary.tcpPort, 1, path2, -1, true);

    sendReplicasToPrimary(primary, replica);

    // Now ask replica to sync:
    replica.newNRTPoint(primaryVersion2, 0, primary.tcpPort);

    // Make sure it sees all docs that were indexed while it was down:
    assertVersionAndHits(primary, primaryVersion2, 110);

    docs.close();
    replica.close();
    primary.close();
  }

  @Nightly
  public void testFullClusterCrash() throws Exception {

    Path path1 = createTempDir("1");
    NodeProcess primary = startNode(-1, 0, path1, -1, true);

    Path path2 = createTempDir("2");
    NodeProcess replica1 = startNode(primary.tcpPort, 1, path2, -1, true);

    Path path3 = createTempDir("3");
    NodeProcess replica2 = startNode(primary.tcpPort, 2, path3, -1, true);

    sendReplicasToPrimary(primary, replica1, replica2);

    // Index 50 docs into primary:
    LineFileDocs docs = new LineFileDocs(random());
    long primaryVersion1 = 0;
    for (int iter=0;iter<5;iter++) {
      try (Connection c = new Connection(primary.tcpPort)) {
        c.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
        for(int i=0;i<10;i++) {
          Document doc = docs.nextDoc();
          primary.addOrUpdateDocument(c, doc, false);
        }
      }

      // Refresh primary, which also pushes to replicas:
      primaryVersion1 = primary.flush(0);
      assertTrue(primaryVersion1 > 0);
    }

    // Wait for replicas to sync up:
    waitForVersionAndHits(replica1, primaryVersion1, 50);
    waitForVersionAndHits(replica2, primaryVersion1, 50);

    primary.commit();
    replica1.commit();
    replica2.commit();

    // Index 10 more docs, but don't sync to replicas:
    try (Connection c = new Connection(primary.tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
      for(int i=0;i<10;i++) {
        Document doc = docs.nextDoc();
        primary.addOrUpdateDocument(c, doc, false);
      }
    }

    // Full cluster crash
    primary.crash();
    replica1.crash();
    replica2.crash();

    // Full cluster restart
    primary = startNode(-1, 0, path1, -1, true);
    replica1 = startNode(primary.tcpPort, 1, path2, -1, true);
    replica2 = startNode(primary.tcpPort, 2, path3, -1, true);

    // Only 50 because we didn't commit primary before the crash:
    
    // It's -1 because it's unpredictable how IW changes segments version on init:
    assertVersionAndHits(primary, -1, 50);
    assertVersionAndHits(replica1, primary.initInfosVersion, 50);
    assertVersionAndHits(replica2, primary.initInfosVersion, 50);

    docs.close();
    primary.close();
    replica1.close();
    replica2.close();
  }

  /** Tell primary current replicas. */
  private void sendReplicasToPrimary(NodeProcess primary, NodeProcess... replicas) throws IOException {
    try (Connection c = new Connection(primary.tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_SET_REPLICAS);
      c.out.writeVInt(replicas.length);
      for(int id=0;id<replicas.length;id++) {
        NodeProcess replica = replicas[id];
        c.out.writeVInt(replica.id);
        c.out.writeVInt(replica.tcpPort);
      }
      c.flush();
      c.in.readByte();
    }
  }

  /** Verifies this node is currently searching the specified version with the specified total hit count, or that it eventually does when
   *  keepTrying is true. */
  private void assertVersionAndHits(NodeProcess node, long expectedVersion, int expectedHitCount) throws Exception {
    try (Connection c = new Connection(node.tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_SEARCH_ALL);
      c.flush();
      long version = c.in.readVLong();
      int hitCount = c.in.readVInt();
      if (expectedVersion != -1) {
        assertEquals("wrong searcher version, with hitCount=" + hitCount, expectedVersion, version);
      }
      assertEquals(expectedHitCount, hitCount);
    }
  }

  private void waitForVersionAndHits(NodeProcess node, long expectedVersion, int expectedHitCount) throws Exception {
    try (Connection c = new Connection(node.tcpPort)) {
      while (true) {
        c.out.writeByte(SimplePrimaryNode.CMD_SEARCH_ALL);
        c.flush();
        long version = c.in.readVLong();
        int hitCount = c.in.readVInt();

        if (version == expectedVersion) {
          assertEquals(expectedHitCount, hitCount);
          break;
        }

        assertTrue(version < expectedVersion);
        Thread.sleep(10);
      }
    }
  }

  static void message(String message) {
    long now = System.nanoTime();
    System.out.println(String.format(Locale.ROOT,
                                     "%5.3fs       :     parent [%11s] %s",
                                     (now-Node.globalStartNS)/1000000000.,
                                     Thread.currentThread().getName(),
                                     message));
  }
}
