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

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RateLimitedIndexOutput;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.LuceneTestCase;

class SimpleReplicaNode extends ReplicaNode {
  final int tcpPort;
  final Jobs jobs;

  // Rate limits incoming bytes/sec when fetching files:
  final RateLimiter fetchRateLimiter;
  final AtomicLong bytesSinceLastRateLimiterCheck = new AtomicLong();
  final Random random;

  /** Changes over time, as primary node crashes and moves around */
  int curPrimaryTCPPort;

  public SimpleReplicaNode(Random random, int id, int tcpPort, Path indexPath, long curPrimaryGen, int primaryTCPPort,
                           SearcherFactory searcherFactory, boolean doCheckIndexOnClose) throws IOException {
    super(id, getDirectory(random, id, indexPath, doCheckIndexOnClose), searcherFactory, System.out);
    this.tcpPort = tcpPort;
    this.random = new Random(random.nextLong());

    // Random IO throttling on file copies: 5 - 20 MB/sec:
    double mbPerSec = 5 * (1.0 + 3*random.nextDouble());
    message(String.format(Locale.ROOT, "top: will rate limit file fetch to %.2f MB/sec", mbPerSec));
    fetchRateLimiter = new RateLimiter.SimpleRateLimiter(mbPerSec);
    this.curPrimaryTCPPort = primaryTCPPort;
    
    start(curPrimaryGen);

    // Handles fetching files from primary:
    jobs = new Jobs(this);
    jobs.setName("R" + id + ".copyJobs");
    jobs.setDaemon(true);
    jobs.start();
  }

  @Override
  protected void launch(CopyJob job) {
    jobs.launch(job);
  }

  @Override
  public void close() throws IOException {
    // Can't be sync'd when calling jobs since it can lead to deadlock:
    jobs.close();
    message("top: jobs closed");
    synchronized(mergeCopyJobs) {
      for (CopyJob job : mergeCopyJobs) {
        message("top: cancel merge copy job " + job);
        job.cancel("jobs closing", null);
      }
    }
    super.close();
  }

  @Override
  protected CopyJob newCopyJob(String reason, Map<String,FileMetaData> files, Map<String,FileMetaData> prevFiles,
                               boolean highPriority, CopyJob.OnceDone onceDone) throws IOException {
    Connection c;
    CopyState copyState;

    // Exceptions in here mean something went wrong talking over the socket, which are fine (e.g. primary node crashed):
    try {
      c = new Connection(curPrimaryTCPPort);
      c.out.writeByte(SimplePrimaryNode.CMD_FETCH_FILES);
      c.out.writeVInt(id);
      if (files == null) {
        // No incoming CopyState: ask primary for latest one now
        c.out.writeByte((byte) 1);
        c.flush();
        copyState = TestSimpleServer.readCopyState(c.in);
        files = copyState.files;
      } else {
        c.out.writeByte((byte) 0);
        copyState = null;
      }
    } catch (Throwable t) {
      throw new NodeCommunicationException("exc while reading files to copy", t);
    }

    return new SimpleCopyJob(reason, c, copyState, this, files, highPriority, onceDone);
  }

  static Directory getDirectory(Random random, int id, Path path, boolean doCheckIndexOnClose) throws IOException {
    MockDirectoryWrapper dir = LuceneTestCase.newMockFSDirectory(path);
    
    dir.setAssertNoUnrefencedFilesOnClose(true);
    dir.setCheckIndexOnClose(doCheckIndexOnClose);

    // Corrupt any index files not referenced by current commit point; this is important (increases test evilness) because we may have done
    // a hard crash of the previous JVM writing to this directory and so MDW's corrupt-unknown-files-on-close never ran:
    Node.nodeMessage(System.out, id, "top: corrupt unknown files");
    dir.corruptUnknownFiles();

    return dir;
  }

  static final byte CMD_NEW_NRT_POINT = 0;

  // Sent by primary to replica to pre-copy merge files:
  static final byte CMD_PRE_COPY_MERGE = 17;

  /** Handles incoming request to the naive TCP server wrapping this node */
  void handleOneConnection(ServerSocket ss, AtomicBoolean stop, InputStream is, Socket socket, DataInput in, DataOutput out, BufferedOutputStream bos) throws IOException, InterruptedException {
    //message("one connection: " + socket);
    outer:
    while (true) {
      byte cmd;
      while (true) {
        if (is.available() > 0) {
          break;
        }
        if (stop.get()) {
          return;
        }
        Thread.sleep(10);
      }

      try {
        cmd = in.readByte();
      } catch (EOFException eofe) {
        break;
      }

      switch(cmd) {
      case CMD_NEW_NRT_POINT:
        {
          long version = in.readVLong();
          long newPrimaryGen = in.readVLong();
          Thread.currentThread().setName("recv-" + version);
          curPrimaryTCPPort = in.readInt();
          message("newNRTPoint primaryTCPPort=" + curPrimaryTCPPort + " version=" + version + " newPrimaryGen=" + newPrimaryGen);
          newNRTPoint(newPrimaryGen, version);
        }
        break;

      case SimplePrimaryNode.CMD_GET_SEARCHING_VERSION:
        // This is called when primary has crashed and we need to elect a new primary from all the still running replicas:

        // Tricky: if a sync is just finishing up, i.e. managed to finish copying all files just before we crashed primary, and is now
        // in the process of opening a new reader, we need to wait for it, to be sure we really pick the most current replica:
        if (isCopying()) {
          message("top: getSearchingVersion: now wait for finish sync");
          // TODO: use immediate concurrency instead of polling:
          while (isCopying() && stop.get() == false) {
            Thread.sleep(10);
            message("top: curNRTCopy=" + curNRTCopy);
          }
          message("top: getSearchingVersion: done wait for finish sync");
        }
        if (stop.get() == false) {
          out.writeVLong(getCurrentSearchingVersion());
        } else {
          message("top: getSearchingVersion: stop waiting for finish sync: stop is set");
        }
        break;

      case SimplePrimaryNode.CMD_SEARCH:
        {
          Thread.currentThread().setName("search");
          IndexSearcher searcher = mgr.acquire();
          try {
            long version = ((DirectoryReader) searcher.getIndexReader()).getVersion();
            int hitCount = searcher.count(new TermQuery(new Term("body", "the")));
            //node.message("version=" + version + " searcher=" + searcher);
            out.writeVLong(version);
            out.writeVInt(hitCount);
            bos.flush();
          } finally {
            mgr.release(searcher);
          }
        }
        continue outer;

      case SimplePrimaryNode.CMD_SEARCH_ALL:
        {
          Thread.currentThread().setName("search all");
          IndexSearcher searcher = mgr.acquire();
          try {
            long version = ((DirectoryReader) searcher.getIndexReader()).getVersion();
            int hitCount = searcher.count(new MatchAllDocsQuery());
            //node.message("version=" + version + " searcher=" + searcher);
            out.writeVLong(version);
            out.writeVInt(hitCount);
            bos.flush();
          } finally {
            mgr.release(searcher);
          }
        }
        continue outer;

      case SimplePrimaryNode.CMD_MARKER_SEARCH:
        {
          Thread.currentThread().setName("msearch");
          int expectedAtLeastCount = in.readVInt();
          IndexSearcher searcher = mgr.acquire();
          try {
            long version = ((DirectoryReader) searcher.getIndexReader()).getVersion();
            int hitCount = searcher.count(new TermQuery(new Term("marker", "marker")));
            if (hitCount < expectedAtLeastCount) {
              message("marker search: expectedAtLeastCount=" + expectedAtLeastCount + " but hitCount=" + hitCount);
              TopDocs hits = searcher.search(new TermQuery(new Term("marker", "marker")), expectedAtLeastCount);
              List<Integer> seen = new ArrayList<>();
              for(ScoreDoc hit : hits.scoreDocs) {
                Document doc = searcher.doc(hit.doc);
                seen.add(Integer.parseInt(doc.get("docid").substring(1)));
              }
              Collections.sort(seen);
              message("saw markers:");
              for(int marker : seen) {
                message("saw m" + marker);
              }
            }

            out.writeVLong(version);
            out.writeVInt(hitCount);
            bos.flush();
          } finally {
            mgr.release(searcher);
          }
        }
        continue outer;

      case SimplePrimaryNode.CMD_COMMIT:
        Thread.currentThread().setName("commit");
        commit();
        out.writeByte((byte) 1);
        break;

      case SimplePrimaryNode.CMD_CLOSE:
        Thread.currentThread().setName("close");
        ss.close();
        out.writeByte((byte) 1);
        break outer;

      case CMD_PRE_COPY_MERGE:
        Thread.currentThread().setName("merge copy");

        long newPrimaryGen = in.readVLong();
        curPrimaryTCPPort = in.readVInt();
        Map<String,FileMetaData> files = TestSimpleServer.readFilesMetaData(in);
        message("done reading files to copy files=" + files.keySet());
        AtomicBoolean finished = new AtomicBoolean();
        CopyJob job = launchPreCopyMerge(finished, newPrimaryGen, files);
        message("done launching copy job files=" + files.keySet());

        // Silly keep alive mechanism, else if e.g. we (replica node) crash, the primary
        // won't notice for a very long time:
        boolean success = false;
        try {
          int count = 0;
          while (true) {
            if (finished.get() || stop.get()) {
              break;
            }
            Thread.sleep(10);
            count++;
            if (count == 100) {
              // Once per second or so, we send a keep alive
              message("send merge pre copy keep alive... files=" + files.keySet());

              // To be evil, we sometimes fail to keep-alive, e.g. simulating a long GC pausing us:
              if (random.nextBoolean()) {
                out.writeByte((byte) 0);
                count = 0;
              }
            }
          }

          out.writeByte((byte) 1);
          bos.flush();
          success = true;
        } finally {
          message("done merge copy files=" + files.keySet() + " success=" + success);
        }
        break;

      default:
        throw new IllegalArgumentException("unrecognized cmd=" + cmd);
      }
      bos.flush();

      break;
    }
  }

  @Override
  protected void sendNewReplica() throws IOException {
    message("send new_replica to primary tcpPort=" + curPrimaryTCPPort);
    try (Connection c = new Connection(curPrimaryTCPPort)) {
      c.out.writeByte(SimplePrimaryNode.CMD_NEW_REPLICA);
      c.out.writeVInt(tcpPort);
      c.flush();
      c.s.shutdownOutput();
    } catch (Throwable t) {
      message("ignoring exc " + t + " sending new_replica to primary tcpPort=" + curPrimaryTCPPort);
    }
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext ioContext) throws IOException {
    return new RateLimitedIndexOutput(fetchRateLimiter, super.createTempOutput(prefix, suffix, ioContext));
  }
}
