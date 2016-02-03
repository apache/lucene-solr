package org.apache.lucene.replicator.nrt;

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.lucene.document.Document;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;

import com.carrotsearch.randomizedtesting.SeedUtils;

// nocommit make some explicit failure tests

// MockRandom's .sd file has no index header/footer:
@SuppressCodecs({"MockRandom", "Memory", "Direct", "SimpleText"})
@SuppressSysoutChecks(bugUrl = "Stuff gets printed, important stuff for debugging a failure")
public class TestNRTReplication extends LuceneTestCase {

  /** cwd where we start each child (server) node */
  private Path childTempDir;

  final AtomicLong nodeStartCounter = new AtomicLong();

  /** Launches a child "server" (separate JVM), which is either primary or replica node */
  NodeProcess startNode(int primaryTCPPort, final int id, Path indexPath, boolean isPrimary, long forcePrimaryVersion) throws IOException {
    List<String> cmd = new ArrayList<>();

    cmd.add(System.getProperty("java.home") 
        + System.getProperty("file.separator")
        + "bin"
        + System.getProperty("file.separator")
        + "java");
    cmd.add("-Xmx512m");

    if (primaryTCPPort != -1) {
      cmd.add("-Dtests.nrtreplication.primaryTCPPort=" + primaryTCPPort);
    } else if (isPrimary == false) {
      // We cannot start a replica when there is no primary:
      return null;
    }
    cmd.add("-Dtests.nrtreplication.closeorcrash=false");

    cmd.add("-Dtests.nrtreplication.node=true");
    cmd.add("-Dtests.nrtreplication.nodeid=" + id);
    cmd.add("-Dtests.nrtreplication.startNS=" + Node.globalStartNS);
    cmd.add("-Dtests.nrtreplication.indexpath=" + indexPath);
    if (isPrimary) {
      cmd.add("-Dtests.nrtreplication.isPrimary=true");
      cmd.add("-Dtests.nrtreplication.forcePrimaryVersion=" + forcePrimaryVersion);
    }

    long myPrimaryGen = 0;
    cmd.add("-Dtests.nrtreplication.primaryGen=" + myPrimaryGen);

    // Mixin our own counter because this is called from a fresh thread which means the seed otherwise isn't changing each time we spawn a
    // new node:
    long seed = random().nextLong() * nodeStartCounter.incrementAndGet();

    cmd.add("-Dtests.seed=" + SeedUtils.formatSeed(seed));
    cmd.add("-ea");
    cmd.add("-cp");
    cmd.add(System.getProperty("java.class.path"));
    cmd.add("org.junit.runner.JUnitCore");
    cmd.add(getClass().getName().replace(getClass().getSimpleName(), "SimpleServer"));

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
    boolean willCrash = false;
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
                                           if (exitValue != 0) {
                                             // should fail test
                                             throw new RuntimeException("node " + id + " process had unexpected non-zero exit status=" + exitValue);
                                           }
                                         }
                                       }, r, System.out, null);
    pumper.setName("pump" + id);

    message("top: node=" + id + " started at tcpPort=" + tcpPort + " initCommitVersion=" + initCommitVersion + " initInfosVersion=" + initInfosVersion);
    return new NodeProcess(p, id, tcpPort, pumper, isPrimary, initCommitVersion, initInfosVersion);
  }

  public void testReplicateDeleteAllDocuments() throws Exception {

    Node.globalStartNS = System.nanoTime();
    childTempDir = createTempDir("child");

    message("change thread name from " + Thread.currentThread().getName());
    Thread.currentThread().setName("main");
    
    Path primaryPath = createTempDir("primary");
    NodeProcess primary = startNode(-1, 0, primaryPath, true, -1);

    Path replicaPath = createTempDir("replica");
    NodeProcess replica = startNode(primary.tcpPort, 1, replicaPath, false, -1);

    // Tell primary current replicas:
    try (Connection c = new Connection(primary.tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_SET_REPLICAS);
      c.out.writeVInt(1);
      c.out.writeVInt(replica.id);
      c.out.writeVInt(replica.tcpPort);
      c.flush();
      c.in.readByte();
    }

    // Index 10 docs into primary:
    LineFileDocs docs = new LineFileDocs(random());
    Connection primaryC = new Connection(primary.tcpPort);
    primaryC.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
    for(int i=0;i<10;i++) {
      Document doc = docs.nextDoc();
      primary.addOrUpdateDocument(primaryC, doc, false);
    }

    // Nothing in replica index yet
    Connection replicaC = new Connection(replica.tcpPort);
    replicaC.out.writeByte(SimplePrimaryNode.CMD_SEARCH_ALL);
    replicaC.flush();
    long version1 = replicaC.in.readVLong();
    assertEquals(0L, version1);
    int hitCount = replicaC.in.readVInt();
    assertEquals(0, hitCount);

    // Refresh primary, which also pushes to replica:
    long primaryVersion1 = primary.flush();
    assertTrue(primaryVersion1 > 0);

    long version2;

    // Wait for replica to show the change
    while (true) {
      replicaC.out.writeByte(SimplePrimaryNode.CMD_SEARCH_ALL);
      replicaC.flush();
      version2 = replicaC.in.readVLong();
      hitCount = replicaC.in.readVInt();
      if (version2 == primaryVersion1) {
        assertEquals(10, hitCount);
        // good!
        break;
      }
      Thread.sleep(10);
    }

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
    replicaC.out.writeByte(SimplePrimaryNode.CMD_SEARCH_ALL);
    replicaC.flush();
    long version3 = replicaC.in.readVLong();
    assertEquals(version2, version3);
    hitCount = replicaC.in.readVInt();
    assertEquals(10, hitCount);
    
    // Refresh primary, which also pushes to replica:
    long primaryVersion2 = primary.flush();
    assertTrue(primaryVersion2 > primaryVersion1);

    // Wait for replica to show the change
    long version4;
    while (true) {
      replicaC.out.writeByte(SimplePrimaryNode.CMD_SEARCH_ALL);
      replicaC.flush();
      version4 = replicaC.in.readVLong();
      hitCount = replicaC.in.readVInt();
      if (version4 == primaryVersion2) {
        assertTrue(version4 > version3);
        assertEquals(0, hitCount);
        // good!
        break;
      }
      Thread.sleep(10);
    }

    // Index 10 docs again:
    for(int i=0;i<10;i++) {
      Document doc = docs.nextDoc();
      primary.addOrUpdateDocument(primaryC, doc, false);
    }

    // Refresh primary, which also pushes to replica:
    long primaryVersion3 = primary.flush();
    assertTrue(primaryVersion3 > primaryVersion2);

    // Wait for replica to show the change
    while (true) {
      replicaC.out.writeByte(SimplePrimaryNode.CMD_SEARCH_ALL);
      replicaC.flush();
      long version5 = replicaC.in.readVLong();
      hitCount = replicaC.in.readVInt();
      if (version5 == primaryVersion3) {
        assertEquals(10, hitCount);
        assertTrue(version5 > version4);
        // good!
        break;
      }
      Thread.sleep(10);
    }

    replicaC.close();
    primaryC.close();

    replica.close();
    primary.close();
  }

  public void testReplicateForceMerge() throws Exception {

    Node.globalStartNS = System.nanoTime();
    childTempDir = createTempDir("child");

    message("change thread name from " + Thread.currentThread().getName());
    Thread.currentThread().setName("main");
    
    Path primaryPath = createTempDir("primary");
    NodeProcess primary = startNode(-1, 0, primaryPath, true, -1);

    Path replicaPath = createTempDir("replica");
    NodeProcess replica = startNode(primary.tcpPort, 1, replicaPath, false, -1);

    // Tell primary current replicas:
    try (Connection c = new Connection(primary.tcpPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_SET_REPLICAS);
      c.out.writeVInt(1);
      c.out.writeVInt(replica.id);
      c.out.writeVInt(replica.tcpPort);
      c.flush();
      c.in.readByte();
    }

    // Index 10 docs into primary:
    LineFileDocs docs = new LineFileDocs(random());
    Connection primaryC = new Connection(primary.tcpPort);
    primaryC.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
    for(int i=0;i<10;i++) {
      Document doc = docs.nextDoc();
      primary.addOrUpdateDocument(primaryC, doc, false);
    }

    // Refresh primary, which also pushes to replica:
    long primaryVersion1 = primary.flush();
    assertTrue(primaryVersion1 > 0);

    // Index 10 more docs into primary:
    for(int i=0;i<10;i++) {
      Document doc = docs.nextDoc();
      primary.addOrUpdateDocument(primaryC, doc, false);
    }

    // Refresh primary, which also pushes to replica:
    long primaryVersion2 = primary.flush();
    assertTrue(primaryVersion2 > primaryVersion1);

    primary.forceMerge(primaryC);

    // Refresh primary, which also pushes to replica:
    long primaryVersion3 = primary.flush();
    assertTrue(primaryVersion3 > primaryVersion2);

    Connection replicaC = new Connection(replica.tcpPort);

    // Wait for replica to show the change
    while (true) {
      replicaC.out.writeByte(SimplePrimaryNode.CMD_SEARCH_ALL);
      replicaC.flush();
      long version = replicaC.in.readVLong();
      int hitCount = replicaC.in.readVInt();
      if (version == primaryVersion3) {
        assertEquals(20, hitCount);
        // good!
        break;
      }
      Thread.sleep(10);
    }

    replicaC.close();
    primaryC.close();

    replica.close();
    primary.close();
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
