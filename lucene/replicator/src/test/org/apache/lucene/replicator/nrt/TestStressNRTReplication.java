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
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.ThreadInterruptedException;

import com.carrotsearch.randomizedtesting.SeedUtils;

/*
  TODO
    - fangs
      - sometimes have one replica be really slow at copying / have random pauses (fake GC) / etc.
    - test should not print scary exceptions and then succeed!
    - are the pre-copied-completed-merged files not being cleared in primary?
      - hmm the logic isn't right today?  a replica may skip pulling a given copy state, that recorded the finished merged segments?
    - later
      - since all nodes are local, we could have a different test only impl that just does local file copies instead of via tcp...
      - get all other "single shard" functions working too: this cluster should "act like" a single shard
        - SLM
        - controlled nrt reopen thread / returning long gen on write
        - live field values
      - add indexes
      - replica should also track maxSegmentName its seen, and tap into inflateGens if it's later promoted to primary?
      - if we named index files using segment's ID we wouldn't have file name conflicts after primary crash / rollback?
      - back pressure on indexing if replicas can't keep up?
      - get xlog working on top?  needs to be checkpointed, so we can correlate IW ops to NRT reader version and prune xlog based on commit
        quorum
        - maybe fix IW to return "gen" or "seq id" or "segment name" or something?
      - replica can copy files from other replicas too / use multicast / rsync / something
      - each replica could also pre-open a SegmentReader after pre-copy when warming a merge
      - we can pre-copy newly flushed files too, for cases where reopen rate is low vs IW's flushing because RAM buffer is full
      - opto: pre-copy files as they are written; if they will become CFS, we can build CFS on the replica?
      - what about multiple commit points?
      - fix primary to init directly from an open replica, instead of having to commit/close the replica first
*/

// Tricky cases:
//   - we are pre-copying a merge, then replica starts up part way through, so it misses that pre-copy and must do it on next nrt point
//   - a down replica starts up, but it's "from the future" vs the current primary, and must copy over file names with different contents
//     but referenced by its latest commit point, so it must fully remove that commit ... which is a hazardous window
//   - replica comes up just as the primary is crashing / moving
//   - electing a new primary when a replica is just finishing its nrt sync: we need to wait for it so we are sure to get the "most up to
//     date" replica
//   - replica comes up after merged segment finished so it doesn't copy over the merged segment "promptly" (i.e. only sees it on NRT refresh)

/**
 * Test case showing how to implement NRT replication.  This test spawns a sub-process per-node, running TestNRTReplicationChild.
 *
 * One node is primary, and segments are periodically flushed there, then concurrently the N replica nodes copy the new files over and open new readers, while
 * primary also opens a new reader.
 *
 * Nodes randomly crash and are restarted.  If the primary crashes, a replica is promoted.
 *
 * Merges are currently first finished on the primary and then pre-copied out to replicas with a merged segment warmer so they don't block
 * ongoing NRT reopens.  Probably replicas could do their own merging instead, but this is more complex and may not be better overall
 * (merging takes a lot of IO resources).
 *
 * Slow network is simulated with a RateLimiter.
 */

// MockRandom's .sd file has no index header/footer:
@SuppressCodecs({"MockRandom", "Memory", "Direct", "SimpleText"})
@SuppressSysoutChecks(bugUrl = "Stuff gets printed, important stuff for debugging a failure")
public class TestStressNRTReplication extends LuceneTestCase {

  // Test evilness controls:

  /** Randomly crash the current primary (losing data!) and promote the "next best" replica. */
  static final boolean DO_CRASH_PRIMARY = true;

  /** Randomly crash (JVM core dumps) a replica; it will later randomly be restarted and sync itself. */
  static final boolean DO_CRASH_REPLICA = true;

  /** Randomly gracefully close a replica; it will later be restarted and sync itself. */
  static final boolean DO_CLOSE_REPLICA = true;

  /** Randomly gracefully close the primary; it will later be restarted and sync itself. */
  static final boolean DO_CLOSE_PRIMARY = true;

  /** If false, all child + parent output is interleaved into single stdout/err */
  static final boolean SEPARATE_CHILD_OUTPUT = false;

  /** Randomly crash whole cluster and then restart it */
  static final boolean DO_FULL_CLUSTER_CRASH = true;

  /** True if we randomly flip a bit while copying files out */
  static final boolean DO_BIT_FLIPS_DURING_COPY = true;

  /** Set to a non-null value to force exactly that many nodes; else, it's random. */
  static final Integer NUM_NODES = null;

  final AtomicBoolean failed = new AtomicBoolean();

  final AtomicBoolean stop = new AtomicBoolean();

  /** cwd where we start each child (server) node */
  private Path childTempDir;

  long primaryGen;

  volatile long lastPrimaryVersion;

  volatile NodeProcess primary;
  volatile NodeProcess[] nodes;
  volatile long[] nodeTimeStamps;
  volatile boolean[] starting;
  
  Path[] indexPaths;

  Path transLogPath;
  SimpleTransLog transLog;
  final AtomicInteger markerUpto = new AtomicInteger();
  final AtomicInteger markerID = new AtomicInteger();

  /** Maps searcher version to how many hits the query body:the matched. */
  final Map<Long,Integer> hitCounts = new ConcurrentHashMap<>();

  /** Maps searcher version to how many marker documents matched.  This should only ever grow (we never delete marker documents). */
  final Map<Long,Integer> versionToMarker = new ConcurrentHashMap<>();

  /** Maps searcher version to xlog location when refresh of this version started. */
  final Map<Long,Long> versionToTransLogLocation = new ConcurrentHashMap<>();

  final AtomicLong nodeStartCounter = new AtomicLong();

  final Set<Integer> crashingNodes = Collections.synchronizedSet(new HashSet<>());

  @Nightly
  public void test() throws Exception {

    Node.globalStartNS = System.nanoTime();

    message("change thread name from " + Thread.currentThread().getName());
    Thread.currentThread().setName("main");

    childTempDir = createTempDir("child");

    // We are parent process:

    // Silly bootstrapping:
    versionToTransLogLocation.put(0L, 0L);

    versionToMarker.put(0L, 0);

    int numNodes;

    if (NUM_NODES == null) {
      numNodes = TestUtil.nextInt(random(), 2, 10);
    } else {
      numNodes = NUM_NODES.intValue();
    }

    System.out.println("TEST: using " + numNodes + " nodes");

    transLogPath = createTempDir("NRTReplication").resolve("translog");
    transLog = new SimpleTransLog(transLogPath);

    //state.rateLimiters = new RateLimiter[numNodes];
    indexPaths = new Path[numNodes];
    nodes = new NodeProcess[numNodes];
    nodeTimeStamps = new long[numNodes];
    Arrays.fill(nodeTimeStamps, Node.globalStartNS);
    starting = new boolean[numNodes];
    
    for(int i=0;i<numNodes;i++) {
      indexPaths[i] = createTempDir("index" + i);
    }

    Thread[] indexers = new Thread[TestUtil.nextInt(random(), 1, 3)];
    System.out.println("TEST: launch " + indexers.length + " indexer threads");
    for(int i=0;i<indexers.length;i++) {
      indexers[i] = new IndexThread();
      indexers[i].setName("indexer" + i);
      indexers[i].setDaemon(true);
      indexers[i].start();
    }

    Thread[] searchers = new Thread[TestUtil.nextInt(random(), 1, 3)];
    System.out.println("TEST: launch " + searchers.length + " searcher threads");
    for(int i=0;i<searchers.length;i++) {
      searchers[i] = new SearchThread();
      searchers[i].setName("searcher" + i);
      searchers[i].setDaemon(true);
      searchers[i].start();
    }

    Thread restarter = new RestartThread();
    restarter.setName("restarter");
    restarter.setDaemon(true);
    restarter.start();

    int runTimeSec;
    if (TEST_NIGHTLY) {
      runTimeSec = RANDOM_MULTIPLIER * TestUtil.nextInt(random(), 120, 240);
    } else {
      runTimeSec = RANDOM_MULTIPLIER * TestUtil.nextInt(random(), 45, 120);
    }

    System.out.println("TEST: will run for " + runTimeSec + " sec");

    long endTime = System.nanoTime() + runTimeSec*1000000000L;

    sendReplicasToPrimary();

    while (failed.get() == false && System.nanoTime() < endTime) {

      // Wait a bit:
      Thread.sleep(TestUtil.nextInt(random(), Math.min(runTimeSec*4, 200), runTimeSec*4));
      if (primary != null && random().nextBoolean()) {
        NodeProcess curPrimary = primary;
        if (curPrimary != null) {

          // Save these before we start flush:
          long nextTransLogLoc = transLog.getNextLocation();
          int markerUptoSav = markerUpto.get();
          message("top: now flush primary; at least marker count=" + markerUptoSav);

          long result;
          try {
            result = primary.flush(markerUptoSav);
          } catch (Throwable t) {
            message("top: flush failed; skipping: " + t.getMessage());
            result = -1;
          }
          if (result > 0) {
            // There were changes
            message("top: flush finished with changed; new primary version=" + result);
            lastPrimaryVersion = result;
            addTransLogLoc(lastPrimaryVersion, nextTransLogLoc);
            addVersionMarker(lastPrimaryVersion, markerUptoSav);
          }
        }
      }

      StringBuilder sb = new StringBuilder();
      int liveCount = 0;
      for(int i=0;i<nodes.length;i++) {
        NodeProcess node = nodes[i];
        if (node != null) {
          if (sb.length() != 0) {
            sb.append(" ");
          }
          liveCount++;
          if (node.isPrimary) {
            sb.append('P');
          } else {
            sb.append('R');
          }
          sb.append(i);
        }
      }

      message("PG=" + (primary == null ? "X" : primaryGen) + " " + liveCount + " (of " + nodes.length + ") nodes running: " + sb);

      // Commit a random node, primary or replica

      if (random().nextInt(10) == 1) {
        NodeProcess node = nodes[random().nextInt(nodes.length)];
        if (node != null && node.nodeIsClosing.get() == false) {
          // TODO: if this node is primary, it means we committed an unpublished version (not exposed as an NRT point)... not sure it matters.
          // maybe we somehow allow IW to commit a specific sis (the one we just flushed)?
          message("top: now commit node=" + node);
          try {
            node.commitAsync();
          } catch (Throwable t) {
            message("top: hit exception during commit with R" + node.id + "; skipping");
            t.printStackTrace(System.out);
          }
        }
      }
    }

    message("TEST: top: test done, now close");
    stop.set(true);
    for(Thread thread : indexers) {
      thread.join();
    }
    for(Thread thread : searchers) {
      thread.join();
    }
    restarter.join();

    // Close replicas before primary so we cancel any in-progres replications:
    System.out.println("TEST: top: now close replicas");
    List<Closeable> toClose = new ArrayList<>();
    for(NodeProcess node : nodes) {
      if (node != primary && node != null) {
        toClose.add(node);
      }
    }
    IOUtils.close(toClose);
    IOUtils.close(primary);
    IOUtils.close(transLog);

    if (failed.get() == false) {
      message("TEST: top: now checkIndex");    
      for(Path path : indexPaths) {
        message("TEST: check " + path);
        MockDirectoryWrapper dir = newMockFSDirectory(path);
        // Just too slow otherwise
        dir.setCrossCheckTermVectorsOnClose(false);
        dir.close();
      }
    } else {
      message("TEST: failed; skip checkIndex");
    }
  }

  private boolean anyNodesStarting() {
    for(int id=0;id<nodes.length;id++) {
      if (starting[id]) {
        return true;
      }
    }

    return false;
  }

  /** Picks a replica and promotes it as new primary. */
  private void promoteReplica() throws IOException {
    message("top: primary crashed; now pick replica to promote");
    long maxSearchingVersion = -1;
    NodeProcess replicaToPromote = null;

    // We must promote the most current replica, because otherwise file name reuse can cause a replication to fail when it needs to copy
    // over a file currently held open for searching.  This also minimizes recovery work since the most current replica means less xlog
    // replay to catch up:
    for (NodeProcess node : nodes) {
      if (node != null) {
        message("ask " + node + " for its current searching version");
        long searchingVersion;
        try {
          searchingVersion = node.getSearchingVersion();
        } catch (Throwable t) {
          message("top: hit SocketException during getSearchingVersion with R" + node.id + "; skipping");
          t.printStackTrace(System.out);
          continue;
        }
        message(node + " has searchingVersion=" + searchingVersion);
        if (searchingVersion > maxSearchingVersion) {
          maxSearchingVersion = searchingVersion;
          replicaToPromote = node;
        }
      }
    }

    if (replicaToPromote == null) {
      message("top: no replicas running; skipping primary promotion");
      return;
    }

    message("top: promote " + replicaToPromote + " version=" + maxSearchingVersion + "; now commit");
    try {
      replicaToPromote.commit();
    } catch (Throwable t) {
      // Something wrong with this replica; skip it:
      message("top: hit exception during commit with R" + replicaToPromote.id + "; skipping");
      t.printStackTrace(System.out);
      return;
    }

    message("top: now shutdown " + replicaToPromote);
    if (replicaToPromote.shutdown() == false) {
      message("top: shutdown failed for R" + replicaToPromote.id + "; skipping primary promotion");
      return;
    }

    int id = replicaToPromote.id;
    message("top: now startPrimary " + replicaToPromote);
    startPrimary(replicaToPromote.id);
  }

  void startPrimary(int id) throws IOException {
    message(id + ": top: startPrimary lastPrimaryVersion=" + lastPrimaryVersion);
    assert nodes[id] == null;

    // Force version of new primary to advance beyond where old primary was, so we never re-use versions.  It may have
    // already advanced beyond newVersion, e.g. if it flushed new segments while during xlog replay:

    // First start node as primary (it opens an IndexWriter) but do not publish it for searching until we replay xlog:
    NodeProcess newPrimary = startNode(id, indexPaths[id], true, lastPrimaryVersion+1);
    if (newPrimary == null) {
      message("top: newPrimary failed to start; abort");
      return;
    }

    // Get xlog location that this node was guaranteed to already have indexed through; this may replay some ops already indexed but it's OK
    // because the ops are idempotent: we updateDocument (by docid) on replay even for original addDocument:
    Long startTransLogLoc;
    Integer markerCount;
    if (newPrimary.initCommitVersion == 0) {
      startTransLogLoc = 0L;
      markerCount = 0;
    } else {
      startTransLogLoc = versionToTransLogLocation.get(newPrimary.initCommitVersion);
      markerCount = versionToMarker.get(newPrimary.initCommitVersion);
    }
    assert startTransLogLoc != null: "newPrimary.initCommitVersion=" + newPrimary.initCommitVersion + " is missing from versionToTransLogLocation: keys=" + versionToTransLogLocation.keySet();
    assert markerCount != null: "newPrimary.initCommitVersion=" + newPrimary.initCommitVersion + " is missing from versionToMarker: keys=" + versionToMarker.keySet();

    // When the primary starts, the userData in its latest commit point tells us which version it had indexed up to, so we know where to
    // replay from in the xlog.  However, we forcefuly advance the version, and then IW on init (or maybe getReader) also adds 1 to it.
    // Since we publish the primary in this state (before xlog replay is done), a replica can start up at this point and pull this version,
    // and possibly later be chosen as a primary, causing problems if the version is known recorded in the translog map.  So we record it
    // here:

    addTransLogLoc(newPrimary.initInfosVersion, startTransLogLoc);
    addVersionMarker(newPrimary.initInfosVersion, markerCount);

    assert newPrimary.initInfosVersion >= lastPrimaryVersion;
    message("top: now change lastPrimaryVersion from " + lastPrimaryVersion + " to " + newPrimary.initInfosVersion + "; startup marker count " + markerCount);
    lastPrimaryVersion = newPrimary.initInfosVersion;

    long nextTransLogLoc = transLog.getNextLocation();
    long t0 = System.nanoTime();
    message("top: start translog replay " + startTransLogLoc + " (version=" + newPrimary.initCommitVersion + ") to " + nextTransLogLoc + " (translog end)");
    try {
      transLog.replay(newPrimary, startTransLogLoc, nextTransLogLoc);
    } catch (IOException ioe) {
      message("top: replay xlog failed; shutdown new primary");
      ioe.printStackTrace(System.out);
      newPrimary.shutdown();
      return;
    }

    long t1 = System.nanoTime();
    message("top: done translog replay; took " + ((t1 - t0)/1000000.0) + " msec; now publish primary");

    // Publish new primary only after translog has succeeded in replaying; this is important, for this test anyway, so we keep a "linear"
    // history so enforcing marker counts is correct.  E.g., if we publish first and replay translog concurrently with incoming ops, then
    // a primary commit that happens while translog is still replaying will incorrectly record the translog loc into the commit user data
    // when in fact that commit did NOT reflect all prior ops.  So if we crash and start up again from that commit point, we are missing
    // ops.
    nodes[id] = newPrimary;
    primary = newPrimary;

    sendReplicasToPrimary();

  }

  /** Launches a child "server" (separate JVM), which is either primary or replica node */
  @SuppressForbidden(reason = "ProcessBuilder requires java.io.File for CWD")
  NodeProcess startNode(final int id, Path indexPath, boolean isPrimary, long forcePrimaryVersion) throws IOException {
    nodeTimeStamps[id] = System.nanoTime();
    List<String> cmd = new ArrayList<>();

    NodeProcess curPrimary = primary;

    cmd.add(System.getProperty("java.home") 
        + System.getProperty("file.separator")
        + "bin"
        + System.getProperty("file.separator")
        + "java");
    cmd.add("-Xmx512m");

    if (curPrimary != null) {
      cmd.add("-Dtests.nrtreplication.primaryTCPPort=" + curPrimary.tcpPort);
    } else if (isPrimary == false) {
      // We cannot start a replica when there is no primary:
      return null;
    }

    // This is very costly (takes more time to check than it did to index); we do this ourselves in the end instead of each time a replica
    // is restarted:
    // cmd.add("-Dtests.nrtreplication.checkonclose=true");

    cmd.add("-Dtests.nrtreplication.node=true");
    cmd.add("-Dtests.nrtreplication.nodeid=" + id);
    cmd.add("-Dtests.nrtreplication.startNS=" + Node.globalStartNS);
    cmd.add("-Dtests.nrtreplication.indexpath=" + indexPath);
    if (isPrimary) {
      cmd.add("-Dtests.nrtreplication.isPrimary=true");
      cmd.add("-Dtests.nrtreplication.forcePrimaryVersion=" + forcePrimaryVersion);
      if (DO_CRASH_PRIMARY) {
        cmd.add("-Dtests.nrtreplication.doRandomCrash=true");
      }
      if (DO_CLOSE_PRIMARY) {
        cmd.add("-Dtests.nrtreplication.doRandomClose=true");
      }
    } else {
      if (DO_CRASH_REPLICA) {
        cmd.add("-Dtests.nrtreplication.doRandomCrash=true");
      }
      if (DO_CLOSE_REPLICA) {
        cmd.add("-Dtests.nrtreplication.doRandomClose=true");
      }
    }

    if (DO_BIT_FLIPS_DURING_COPY) {
      cmd.add("-Dtests.nrtreplication.doFlipBitsDuringCopy=true");
    }

    long myPrimaryGen = primaryGen;
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

    Writer childLog;

    if (SEPARATE_CHILD_OUTPUT) {
      Path childOut = childTempDir.resolve(id + ".log");
      message("logging to " + childOut);
      childLog = Files.newBufferedWriter(childOut, StandardCharsets.UTF_8, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
      childLog.write("\n\nSTART NEW CHILD:\n");
    } else {
      childLog = null;
    }

    //message("child process command: " + cmd);
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
        if (p.exitValue() == 0) {
          message("zero exit status; assuming failed to remove segments_N; skipping");
          return null;
        }

        // Hackity hack, in case primary crashed/closed and we haven't noticed (reaped the process) yet:
        if (isPrimary == false) {
          for(int i=0;i<100;i++) {
            NodeProcess primary2 = primary;
            if (primaryGen != myPrimaryGen || primary2 == null || primary2.nodeIsClosing.get()) {
              // OK: primary crashed while we were trying to start, so it's expected/allowed that we could not start the replica:
              message("primary crashed/closed while replica R" + id + " tried to start; skipping");
              return null;
            } else {
              try {
                Thread.sleep(10);
              } catch (InterruptedException ie) {
                throw new ThreadInterruptedException(ie);
              }
            }
          }
        }

        // Should fail the test:
        message("top: now fail test replica R" + id + " failed to start");
        failed.set(true);
        throw new RuntimeException("replica R" + id + " failed to start");
      }

      if (childLog != null) {
        childLog.write(l);
        childLog.write("\n");
        childLog.flush();
      } else if (logTimeStart.matcher(l).matches()) {
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
      }
    }

    final boolean finalWillCrash = willCrash;
    final AtomicBoolean nodeIsClosing = new AtomicBoolean();

    // Baby sits the child process, pulling its stdout and printing to our stdout, calling nodeClosed once it exits:
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
                                           if (childLog != null) {
                                             try {
                                               childLog.write("process done; exitValue=" + exitValue + "\n");
                                               childLog.close();
                                             } catch (IOException ioe) {
                                               throw new RuntimeException(ioe);
                                             }
                                           }
                                           if (exitValue != 0 && finalWillCrash == false && crashingNodes.remove(id) == false) {
                                             // should fail test
                                             failed.set(true);
                                             if (childLog != null) {
                                               throw new RuntimeException("node " + id + " process had unexpected non-zero exit status=" + exitValue + "; see " + childLog + " for details");
                                             } else {
                                               throw new RuntimeException("node " + id + " process had unexpected non-zero exit status=" + exitValue);
                                             }
                                           }
                                           nodeClosed(id);
                                         }
                                       }, r, System.out, childLog, nodeIsClosing);
    pumper.setName("pump" + id);

    message("top: node=" + id + " started at tcpPort=" + tcpPort + " initCommitVersion=" + initCommitVersion + " initInfosVersion=" + initInfosVersion);
    return new NodeProcess(p, id, tcpPort, pumper, isPrimary, initCommitVersion, initInfosVersion, nodeIsClosing);
  }

  private void nodeClosed(int id) {
    NodeProcess oldNode = nodes[id];
    if (primary != null && oldNode == primary) {
      message("top: " + primary + ": primary process finished");
      primary = null;
      primaryGen++;
    } else {
      message("top: " + oldNode + ": replica process finished");
    }
    if (oldNode != null) {
      oldNode.isOpen = false;
    }
    nodes[id] = null;
    nodeTimeStamps[id] = System.nanoTime();

    sendReplicasToPrimary();
  }

  /** Sends currently alive replicas to primary, which uses this to know who to notify when it does a refresh */
  private void sendReplicasToPrimary() {
    NodeProcess curPrimary = primary;
    if (curPrimary != null) {
      List<NodeProcess> replicas = new ArrayList<>();
      for (NodeProcess node : nodes) {
        if (node != null && node.isPrimary == false) {
          replicas.add(node);
        }
      }

      message("top: send " + replicas.size() + " replicas to primary");

      try (Connection c = new Connection(curPrimary.tcpPort)) {
        c.out.writeByte(SimplePrimaryNode.CMD_SET_REPLICAS);
        c.out.writeVInt(replicas.size());        
        for(NodeProcess replica : replicas) {
          c.out.writeVInt(replica.id);
          c.out.writeVInt(replica.tcpPort);
        }
        c.flush();
        c.in.readByte();
      } catch (Throwable t) {
        message("top: ignore exc sending replicas to primary P" + curPrimary.id + " at tcpPort=" + curPrimary.tcpPort);
        t.printStackTrace(System.out);
      }
    }
  }

  void addVersionMarker(long version, int count) {
    //System.out.println("ADD VERSION MARKER version=" + version + " count=" + count);
    if (versionToMarker.containsKey(version)) {
      int curCount = versionToMarker.get(version);
      if (curCount != count) {
        message("top: wrong marker count version=" + version + " count=" + count + " curCount=" + curCount);
        throw new IllegalStateException("version=" + version + " count=" + count + " curCount=" + curCount);
      }
    } else {
      message("top: record marker count: version=" + version + " count=" + count);
      versionToMarker.put(version, count);
    }
  }

  void addTransLogLoc(long version, long loc) {
    message("top: record transLogLoc: version=" + version + " loc=" + loc);
    versionToTransLogLocation.put(version, loc);
  }

  // Periodically wakes up and starts up any down nodes:
  private class RestartThread extends Thread {
    @Override
    public void run() {

      List<Thread> startupThreads = Collections.synchronizedList(new ArrayList<>());

      try {
        while (stop.get() == false) {
          Thread.sleep(TestUtil.nextInt(random(), 50, 500));
          //message("top: restarter cycle");

          // Randomly crash full cluster:
          if (DO_FULL_CLUSTER_CRASH && random().nextInt(500) == 17) {
            message("top: full cluster crash");
            for(int i=0;i<nodes.length;i++) {
              if (starting[i]) {
                message("N" + i + ": top: wait for startup so we can crash...");
                while (starting[i]) {
                  Thread.sleep(10);
                }
                message("N" + i + ": top: done wait for startup");
              }
              NodeProcess node = nodes[i];
              if (node != null) {
                crashingNodes.add(i);
                message("top: N" + node.id + ": top: now crash node");
                node.crash();
                message("top: N" + node.id + ": top: done crash node");
              }
            }
          }

          List<Integer> downNodes = new ArrayList<>();
          StringBuilder b = new StringBuilder();
          long nowNS = System.nanoTime();
          for(int i=0;i<nodes.length;i++) {
            b.append(' ');
            double sec = (nowNS - nodeTimeStamps[i])/1000000000.0;
            String prefix;
            if (nodes[i] == null) {
              downNodes.add(i);
              if (starting[i]) {
                prefix = "s";
              } else {
                prefix = "x";
              }
            } else {
              prefix = "";
            }
            if (primary != null && nodes[i] == primary) {
              prefix += "p";
            }
            b.append(String.format(Locale.ROOT, "%s%d(%.1fs)", prefix, i, sec));
          }
          message("node status" + b.toString());
          message("downNodes=" + downNodes);

          // If primary is down, promote a replica:
          if (primary == null) {
            if (anyNodesStarting()) {
              message("top: skip promote replica: nodes are still starting");
              continue;
            }
            promoteReplica();
          }

          // Randomly start up a down a replica:

          // Stop or start a replica
          if (downNodes.isEmpty() == false) {
            int idx = downNodes.get(random().nextInt(downNodes.size()));
            if (starting[idx] == false) {
              if (primary == null) {
                if (downNodes.size() == nodes.length) {
                  // Cold start: entire cluster is down, start this node up as the new primary
                  message("N" + idx + ": top: cold start as primary");
                  startPrimary(idx);
                }
              } else if (random().nextDouble() < ((double) downNodes.size())/nodes.length) {
                // Start up replica:
                starting[idx] = true;
                message("N" + idx + ": top: start up: launch thread");
                Thread t = new Thread() {
                    @Override
                    public void run() {
                      try {
                        message("N" + idx + ": top: start up thread");
                        nodes[idx] = startNode(idx, indexPaths[idx], false, -1);
                        sendReplicasToPrimary();
                      } catch (Throwable t) {
                        failed.set(true);
                        stop.set(true);
                        throw new RuntimeException(t);
                      } finally {
                        starting[idx] = false;
                        startupThreads.remove(Thread.currentThread());
                      }
                    }
                  };
                t.setName("start R" + idx);
                t.start();
                startupThreads.add(t);
              }
            } else {
              message("node " + idx + " still starting");
            }
          }
        }

        System.out.println("Restarter: now stop: join " + startupThreads.size() + " startup threads");

        while (startupThreads.size() > 0) {
          Thread.sleep(10);
        }

      } catch (Throwable t) {
        failed.set(true);
        stop.set(true);
        throw new RuntimeException(t);
      }
    }
  }

  /** Randomly picks a node and runs a search against it */
  private class SearchThread extends Thread {

    @Override
    public void run() {
      // Maps version to number of hits for silly 'the' TermQuery:
      Query theQuery = new TermQuery(new Term("body", "the"));

      // Persists connections
      Map<Integer,Connection> connections = new HashMap<>();

      while (stop.get() == false) {
        NodeProcess node = nodes[random().nextInt(nodes.length)];
        if (node == null || node.isOpen == false) {
          continue;
        }

        if (node.lock.tryLock() == false) {
          // Node is in the process of closing or crashing or something
          continue;
        }

        boolean nodeIsPrimary = node == primary;

        try {

          Thread.currentThread().setName("Searcher node=" + node);

          //System.out.println("S: cycle; conns=" + connections);

          Connection c = connections.get(node.id);

          long version;
          try {
            if (c == null) {
              //System.out.println("S: new connection " + node.id + " " + Thread.currentThread().getName());
              c = new Connection(node.tcpPort);
              connections.put(node.id, c);
            } else {
              //System.out.println("S: reuse connection " + node.id + " " + Thread.currentThread().getName());
            }

            c.out.writeByte(SimplePrimaryNode.CMD_SEARCH);
            c.flush();

            while (c.sockIn.available() == 0) {
              if (stop.get()) {
                break;
              }
              if (node.isOpen == false) {
                throw new IOException("node closed");
              }
              Thread.sleep(1);
            }
            version = c.in.readVLong();

            while (c.sockIn.available() == 0) {
              if (stop.get()) {
                break;
              }
              if (node.isOpen == false) {
                throw new IOException("node closed");
              }
              Thread.sleep(1);
            }
            int hitCount = c.in.readVInt();

            Integer oldHitCount = hitCounts.get(version);

            // TODO: we never prune this map...
            if (oldHitCount == null) {
              hitCounts.put(version, hitCount);
              message("top: searcher: record search hitCount version=" + version + " hitCount=" + hitCount + " node=" + node);
              if (nodeIsPrimary && version > lastPrimaryVersion) {
                // It's possible a search request sees a new primary version because it's in the process of flushing, but then the primary
                // crashes.  In this case we need to ensure new primary forces its version beyond this:
                message("top: searcher: set lastPrimaryVersion=" + lastPrimaryVersion + " vs " + version);
                lastPrimaryVersion = version;
              }
            } else {
              // Just ensure that all nodes show the same hit count for
              // the same version, i.e. they really are replicas of one another:
              if (oldHitCount.intValue() != hitCount) {
                failed.set(true);
                stop.set(true);
                message("top: searcher: wrong version hitCount: version=" + version + " oldHitCount=" + oldHitCount.intValue() + " hitCount=" + hitCount);
                fail("version=" + version + " oldHitCount=" + oldHitCount.intValue() + " hitCount=" + hitCount);
              }
            }
          } catch (IOException ioe) {
            //message("top: searcher: ignore exc talking to node " + node + ": " + ioe);
            //ioe.printStackTrace(System.out);
            IOUtils.closeWhileHandlingException(c);
            connections.remove(node.id);
            continue;
          }

          // This can be null if primary is flushing, has already refreshed its searcher, but is e.g. still notifying replicas and hasn't
          // yet returned the version to us, in which case this searcher thread can see the version before the main thread has added it to
          // versionToMarker:
          Integer expectedAtLeastHitCount = versionToMarker.get(version);

          if (expectedAtLeastHitCount != null && expectedAtLeastHitCount > 0 && random().nextInt(10) == 7) {
            try {
              c.out.writeByte(SimplePrimaryNode.CMD_MARKER_SEARCH);
              c.out.writeVInt(expectedAtLeastHitCount);
              c.flush();
              while (c.sockIn.available() == 0) {
                if (stop.get()) {
                  break;
                }
                if (node.isOpen == false) {
                  throw new IOException("node died");
                }
                Thread.sleep(1);
              }

              version = c.in.readVLong();

              while (c.sockIn.available() == 0) {
                if (stop.get()) {
                  break;
                }
                if (node.isOpen == false) {
                  throw new IOException("node died");
                }
                Thread.sleep(1);
              }

              int hitCount = c.in.readVInt();

              // Look for data loss: make sure all marker docs are visible:
            
              if (hitCount < expectedAtLeastHitCount) {

                String failMessage = "node=" + node + ": documents were lost version=" + version + " hitCount=" + hitCount + " vs expectedAtLeastHitCount=" + expectedAtLeastHitCount;
                message(failMessage);
                failed.set(true);
                stop.set(true);
                fail(failMessage);
              }
            } catch (IOException ioe) {
              //message("top: searcher: ignore exc talking to node " + node + ": " + ioe);
              //throw new RuntimeException(ioe);
              //ioe.printStackTrace(System.out);
              IOUtils.closeWhileHandlingException(c);
              connections.remove(node.id);
              continue;
            }
          }

          Thread.sleep(10);

        } catch (Throwable t) {
          failed.set(true);
          stop.set(true);
          throw new RuntimeException(t);
        } finally {
          node.lock.unlock();
        }
      }
      System.out.println("Searcher: now stop");
      IOUtils.closeWhileHandlingException(connections.values());
    }
  }

  private class IndexThread extends Thread {

    @Override
    public void run() {

      try {
        LineFileDocs docs = new LineFileDocs(random());
        int docCount = 0;

        // How often we do an update/delete vs add:
        double updatePct = random().nextDouble();

        // Varies how many docs/sec we index:
        int sleepChance = TestUtil.nextInt(random(), 4, 100);

        message("top: indexer: updatePct=" + updatePct + " sleepChance=" + sleepChance);

        long lastTransLogLoc = transLog.getNextLocation();
        
        NodeProcess curPrimary = null;
        Connection c = null;

        while (stop.get() == false) {

          try {
            while (stop.get() == false && curPrimary == null) {
              Thread.sleep(10);
              curPrimary = primary;
              if (curPrimary != null) {
                c = new Connection(curPrimary.tcpPort);
                c.out.writeByte(SimplePrimaryNode.CMD_INDEXING);
                break;
              }
            }

            if (stop.get()) {
              break;
            }

            Thread.currentThread().setName("indexer p" + curPrimary.id);

            if (random().nextInt(10) == 7) {
              // We use the marker docs to check for data loss in search thread:
              Document doc = new Document();
              int id = markerID.getAndIncrement();
              String idString = "m"+id;
              doc.add(newStringField("docid", idString, Field.Store.YES));
              doc.add(newStringField("marker", "marker", Field.Store.YES));
              curPrimary.addOrUpdateDocument(c, doc, false);
              transLog.addDocument(idString, doc);
              // Only increment after primary replies:
              markerUpto.getAndIncrement();
              //message("index marker=" + idString + "; translog is " + Node.bytesToString(Files.size(transLogPath)));
            }

            if (docCount > 0 && random().nextDouble() < updatePct) {
              int randomID = random().nextInt(docCount);
              String randomIDString = Integer.toString(randomID);
              if (random().nextBoolean()) {
                // Replace previous doc
                Document doc = docs.nextDoc();
                ((Field) doc.getField("docid")).setStringValue(randomIDString);
                curPrimary.addOrUpdateDocument(c, doc, true);
                transLog.updateDocument(randomIDString, doc);
              } else {
                // Delete previous doc
                curPrimary.deleteDocument(c, randomIDString);
                transLog.deleteDocuments(randomIDString);
              }
            } else {
              // Add new doc:
              Document doc = docs.nextDoc();
              String idString = Integer.toString(docCount++);
              ((Field) doc.getField("docid")).setStringValue(idString);
              curPrimary.addOrUpdateDocument(c, doc, false);
              transLog.addDocument(idString, doc);
            }
          } catch (IOException se) {
            // Assume primary crashed
            if (c != null) {
              message("top: indexer lost connection to primary");
            }
            try {
              c.close();
            } catch (Throwable t) {
            }
            curPrimary = null;
            c = null;
          }

          if (random().nextInt(sleepChance) == 0) {
            Thread.sleep(10);
          }

          if (random().nextInt(100) == 17) {
            int pauseMS = TestUtil.nextInt(random(), 500, 2000);
            System.out.println("Indexer: now pause for " + pauseMS + " msec...");
            Thread.sleep(pauseMS);
            System.out.println("Indexer: done pause for a bit...");
          }
        }
        if (curPrimary != null) {
          try {
            c.out.writeByte(SimplePrimaryNode.CMD_INDEXING_DONE);
            c.flush();
            c.in.readByte();
          } catch (IOException se) {
            // Assume primary crashed
            message("top: indexer lost connection to primary");
            try {
              c.close();
            } catch (Throwable t) {
            }
            curPrimary = null;
            c = null;
          }
        }
        System.out.println("Indexer: now stop");
      } catch (Throwable t) {
        failed.set(true);
        stop.set(true);
        throw new RuntimeException(t);
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

  static void message(String message, long localStartNS) {
    long now = System.nanoTime();
    System.out.println(String.format(Locale.ROOT,
                                     "%5.3fs %5.1fs:     parent [%11s] %s",
                                     (now-Node.globalStartNS)/1000000000.,
                                     (now-localStartNS)/1000000000.,
                                     Thread.currentThread().getName(),
                                     message));
  }
}
