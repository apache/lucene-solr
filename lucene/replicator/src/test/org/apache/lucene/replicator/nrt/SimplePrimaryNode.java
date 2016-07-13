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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.ThreadInterruptedException;

/** A primary node that uses simple TCP connections to send commands and copy files */

class SimplePrimaryNode extends PrimaryNode {

  final int tcpPort;

  final Random random;

  // These are updated by parent test process whenever replicas change:
  int[] replicaTCPPorts = new int[0];
  int[] replicaIDs = new int[0];

  // So we only flip a bit once per file name:
  final Set<String> bitFlipped = Collections.synchronizedSet(new HashSet<>());

  final List<MergePreCopy> warmingSegments = Collections.synchronizedList(new ArrayList<>());

  final boolean doFlipBitsDuringCopy;

  static class MergePreCopy {
    final List<Connection> connections = Collections.synchronizedList(new ArrayList<>());
    final Map<String,FileMetaData> files;
    private boolean finished;

    public MergePreCopy(Map<String,FileMetaData> files) {
      this.files = files;
    }

    public synchronized boolean tryAddConnection(Connection c) {
      if (finished == false) {
        connections.add(c);
        return true;
      } else {
        return false;
      }
    }

    public synchronized boolean finished() {
      if (connections.isEmpty()) {
        finished = true;
        return true;
      } else {
        return false;
      }
    }
  }

  public SimplePrimaryNode(Random random, Path indexPath, int id, int tcpPort, long primaryGen, long forcePrimaryVersion, SearcherFactory searcherFactory,
                           boolean doFlipBitsDuringCopy, boolean doCheckIndexOnClose) throws IOException {
    super(initWriter(id, random, indexPath, doCheckIndexOnClose), id, primaryGen, forcePrimaryVersion, searcherFactory, System.out);
    this.tcpPort = tcpPort;
    this.random = new Random(random.nextLong());
    this.doFlipBitsDuringCopy = doFlipBitsDuringCopy;
  }

  /** Records currently alive replicas. */
  public synchronized void setReplicas(int[] replicaIDs, int[] replicaTCPPorts) {
    message("top: set replicasIDs=" + Arrays.toString(replicaIDs) + " tcpPorts=" + Arrays.toString(replicaTCPPorts));
    this.replicaIDs = replicaIDs;
    this.replicaTCPPorts = replicaTCPPorts;
  }

  private static IndexWriter initWriter(int id, Random random, Path indexPath, boolean doCheckIndexOnClose) throws IOException {
    Directory dir = SimpleReplicaNode.getDirectory(random, id, indexPath, doCheckIndexOnClose);

    MockAnalyzer analyzer = new MockAnalyzer(random);
    analyzer.setMaxTokenLength(TestUtil.nextInt(random, 1, IndexWriter.MAX_TERM_LENGTH));
    IndexWriterConfig iwc = LuceneTestCase.newIndexWriterConfig(random, analyzer);

    MergePolicy mp = iwc.getMergePolicy();
    //iwc.setInfoStream(new PrintStreamInfoStream(System.out));

    // Force more frequent merging so we stress merge warming:
    if (mp instanceof TieredMergePolicy) {
      TieredMergePolicy tmp = (TieredMergePolicy) mp;
      tmp.setSegmentsPerTier(3);
      tmp.setMaxMergeAtOnce(3);
    } else if (mp instanceof LogMergePolicy) {
      LogMergePolicy lmp = (LogMergePolicy) mp;
      lmp.setMergeFactor(3);
    }

    IndexWriter writer = new IndexWriter(dir, iwc);

    TestUtil.reduceOpenFiles(writer);
    return writer;
  }

  @Override
  protected void preCopyMergedSegmentFiles(SegmentCommitInfo info, Map<String,FileMetaData> files) throws IOException {
    int[] replicaTCPPorts = this.replicaTCPPorts;
    if (replicaTCPPorts == null) {
      message("no replicas; skip warming " + info);
      return;
    }

    message("top: warm merge " + info + " to " + replicaTCPPorts.length + " replicas; tcpPort=" + tcpPort + ": files=" + files.keySet());

    MergePreCopy preCopy = new MergePreCopy(files);
    warmingSegments.add(preCopy);

    try {

      Set<String> fileNames = files.keySet();

      // Ask all currently known replicas to pre-copy this newly merged segment's files:
      for (int replicaTCPPort : replicaTCPPorts) {
        try {
          Connection c = new Connection(replicaTCPPort);
          c.out.writeByte(SimpleReplicaNode.CMD_PRE_COPY_MERGE);
          c.out.writeVLong(primaryGen);
          c.out.writeVInt(tcpPort);
          SimpleServer.writeFilesMetaData(c.out, files);
          c.flush();
          c.s.shutdownOutput();
          message("warm connection " + c.s);
          preCopy.connections.add(c);
        } catch (Throwable t) {
          message("top: ignore exception trying to warm to replica port " + replicaTCPPort + ": " + t);
          //t.printStackTrace(System.out);
        }
      }

      long startNS = System.nanoTime();
      long lastWarnNS = startNS;

      // TODO: maybe ... place some sort of time limit on how long we are willing to wait for slow replica(s) to finish copying?
      while (preCopy.finished() == false) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException ie) {
          throw new ThreadInterruptedException(ie);
        }

        if (isClosed()) {
          message("top: primary is closing: now cancel segment warming");
          synchronized(preCopy.connections) {
            IOUtils.closeWhileHandlingException(preCopy.connections);
          }
          return;
        }

        long ns = System.nanoTime();
        if (ns - lastWarnNS > 1000000000L) {
          message(String.format(Locale.ROOT, "top: warning: still warming merge " + info + " to " + preCopy.connections.size() + " replicas for %.1f sec...", (ns - startNS)/1000000000.0));
          lastWarnNS = ns;
        }

        // Process keep-alives:
        synchronized(preCopy.connections) {
          Iterator<Connection> it = preCopy.connections.iterator();
          while (it.hasNext()) {
            Connection c = it.next();
            try {
              long nowNS = System.nanoTime();
              boolean done = false;
              while (c.sockIn.available() > 0) {
                byte b = c.in.readByte();
                if (b == 0) {
                  // keep-alive
                  c.lastKeepAliveNS = nowNS;
                  message("keep-alive for socket=" + c.s + " merge files=" + files.keySet());
                } else {
                  // merge is done pre-copying to this node
                  if (b != 1) {
                    throw new IllegalArgumentException();
                  }
                  message("connection socket=" + c.s + " is done warming its merge " + info + " files=" + files.keySet());
                  IOUtils.closeWhileHandlingException(c);
                  it.remove();
                  done = true;
                  break;
                }
              }

              // If > 2 sec since we saw a keep-alive, assume this replica is dead:
              if (done == false && nowNS - c.lastKeepAliveNS > 2000000000L) {
                message("top: warning: replica socket=" + c.s + " for segment=" + info + " seems to be dead; closing files=" + files.keySet());
                IOUtils.closeWhileHandlingException(c);
                it.remove();
                done = true;
              }

              if (done == false && random.nextInt(1000) == 17) {
                message("top: warning: now randomly dropping replica from merge warming; files=" + files.keySet());
                IOUtils.closeWhileHandlingException(c);
                it.remove();
                done = true;
              }

            } catch (Throwable t) {
              message("top: ignore exception trying to read byte during warm for segment=" + info + " to replica socket=" + c.s + ": " + t + " files=" + files.keySet());
              IOUtils.closeWhileHandlingException(c);
              it.remove();
            }
          }
        }
      }
    } finally {
      warmingSegments.remove(preCopy);
    }
  }

  /** Flushes all indexing ops to disk and notifies all replicas that they should now copy */
  private void handleFlush(DataInput topIn, DataOutput topOut, BufferedOutputStream bos) throws IOException {
    Thread.currentThread().setName("flush");

    int atLeastMarkerCount = topIn.readVInt();

    int[] replicaTCPPorts;
    int[] replicaIDs;
    synchronized (this) {
      replicaTCPPorts = this.replicaTCPPorts;
      replicaIDs = this.replicaIDs;
    }

    message("now flush; " + replicaIDs.length + " replicas");

    if (flushAndRefresh()) {
      // Something did get flushed (there were indexing ops since the last flush):

      verifyAtLeastMarkerCount(atLeastMarkerCount, null);
 
     // Tell caller the version before pushing to replicas, so that even if we crash after this, caller will know what version we
      // (possibly) pushed to some replicas.  Alternatively we could make this 2 separate ops?
      long version = getCopyStateVersion();
      message("send flushed version=" + version);
      topOut.writeLong(version);
      bos.flush();

      // Notify current replicas:
      for(int i=0;i<replicaIDs.length;i++) {
        int replicaID = replicaIDs[i];
        try (Connection c = new Connection(replicaTCPPorts[i])) {
          message("send NEW_NRT_POINT to R" + replicaID + " at tcpPort=" + replicaTCPPorts[i]);
          c.out.writeByte(SimpleReplicaNode.CMD_NEW_NRT_POINT);
          c.out.writeVLong(version);
          c.out.writeVLong(primaryGen);
          c.out.writeInt(tcpPort);
          c.flush();
          // TODO: we should use multicast to broadcast files out to replicas
          // TODO: ... replicas could copy from one another instead of just primary
          // TODO: we could also prioritize one replica at a time?
        } catch (Throwable t) {
          message("top: failed to connect R" + replicaID + " for newNRTPoint; skipping: " + t.getMessage());
        }
      }
    } else {
      // No changes flushed:
      topOut.writeLong(-getCopyStateVersion());
    }
  }

  /** Pushes CopyState on the wire */
  private static void writeCopyState(CopyState state, DataOutput out) throws IOException {
    // TODO (opto): we could encode to byte[] once when we created the copyState, and then just send same byts to all replicas...
    out.writeVInt(state.infosBytes.length);
    out.writeBytes(state.infosBytes, 0, state.infosBytes.length);
    out.writeVLong(state.gen);
    out.writeVLong(state.version);
    SimpleServer.writeFilesMetaData(out, state.files);

    out.writeVInt(state.completedMergeFiles.size());
    for(String fileName : state.completedMergeFiles) {
      out.writeString(fileName);
    }
    out.writeVLong(state.primaryGen);
  }

  /** Called when another node (replica) wants to copy files from us */
  private boolean handleFetchFiles(Random random, Socket socket, DataInput destIn, DataOutput destOut, BufferedOutputStream bos) throws IOException {
    Thread.currentThread().setName("send");

    int replicaID = destIn.readVInt();
    message("top: start fetch for R" + replicaID + " socket=" + socket);
    byte b = destIn.readByte();
    CopyState copyState;
    if (b == 0) {
      // Caller already has CopyState
      copyState = null;
    } else if (b == 1) {
      // Caller does not have CopyState; we pull the latest one:
      copyState = getCopyState();
      Thread.currentThread().setName("send-R" + replicaID + "-" + copyState.version);
    } else {
      // Protocol error:
      throw new IllegalArgumentException("invalid CopyState byte=" + b);
    }

    try {
      if (copyState != null) {
        // Serialize CopyState on the wire to the client:
        writeCopyState(copyState, destOut);
        bos.flush();
      }

      byte[] buffer = new byte[16384];
      int fileCount = 0;
      long totBytesSent = 0;
      while (true) {
        byte done = destIn.readByte();
        if (done == 1) {
          break;
        } else if (done != 0) {
          throw new IllegalArgumentException("expected 0 or 1 byte but got " + done);
        }

        // Name of the file the replica wants us to send:
        String fileName = destIn.readString();

        // Starting offset in the file we should start sending bytes from:
        long fpStart = destIn.readVLong();

        try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
          long len = in.length();
          //message("fetch " + fileName + ": send len=" + len);
          destOut.writeVLong(len);
          in.seek(fpStart);
          long upto = fpStart;
          while (upto < len) {
            int chunk = (int) Math.min(buffer.length, (len-upto));
            in.readBytes(buffer, 0, chunk);
            if (doFlipBitsDuringCopy) {
              if (random.nextInt(3000) == 17 && bitFlipped.contains(fileName) == false) {
                bitFlipped.add(fileName);
                message("file " + fileName + " to R" + replicaID + ": now randomly flipping a bit at byte=" + upto);
                int x = random.nextInt(chunk);
                int bit = random.nextInt(8);
                buffer[x] ^= 1 << bit;
              }
            }
            destOut.writeBytes(buffer, 0, chunk);
            upto += chunk;
            totBytesSent += chunk;
          }
        }

        fileCount++;
      }

      message("top: done fetch files for R" + replicaID + ": sent " + fileCount + " files; sent " + totBytesSent + " bytes");
    } catch (Throwable t) {
      message("top: exception during fetch: " + t.getMessage() + "; now close socket");
      socket.close();
      return false;
    } finally {
      if (copyState != null) {
        message("top: fetch: now release CopyState");
        releaseCopyState(copyState);
      }
    }

    return true;
  }

  static final FieldType tokenizedWithTermVectors;

  static {
    tokenizedWithTermVectors = new FieldType(TextField.TYPE_STORED);
    tokenizedWithTermVectors.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    tokenizedWithTermVectors.setStoreTermVectors(true);
    tokenizedWithTermVectors.setStoreTermVectorOffsets(true);
    tokenizedWithTermVectors.setStoreTermVectorPositions(true);
  }

  private void handleIndexing(Socket socket, AtomicBoolean stop, InputStream is, DataInput in, DataOutput out, BufferedOutputStream bos) throws IOException, InterruptedException {
    Thread.currentThread().setName("indexing");
    message("start handling indexing socket=" + socket);
    while (true) {
      while (true) {
        if (is.available() > 0) {
          break;
        }
        if (stop.get()) {
          return;
        }
        Thread.sleep(10);
      }
      byte cmd;
      try {
        cmd = in.readByte();
      } catch (EOFException eofe) {
        // done
        return;
      }
      //message("INDEXING OP " + cmd);
      if (cmd == CMD_ADD_DOC) {
        handleAddDocument(in, out);
        out.writeByte((byte) 1);
        bos.flush();
      } else if (cmd == CMD_UPDATE_DOC) {
        handleUpdateDocument(in, out);
        out.writeByte((byte) 1);
        bos.flush();
      } else if (cmd == CMD_DELETE_DOC) {
        handleDeleteDocument(in, out);
        out.writeByte((byte) 1);
        bos.flush();
      } else if (cmd == CMD_DELETE_ALL_DOCS) {
        writer.deleteAll();
        out.writeByte((byte) 1);
        bos.flush();
      } else if (cmd == CMD_FORCE_MERGE) {
        writer.forceMerge(1);
        out.writeByte((byte) 1);
        bos.flush();
      } else if (cmd == CMD_INDEXING_DONE) {
        out.writeByte((byte) 1);
        bos.flush();
        break;
      } else {
        throw new IllegalArgumentException("cmd must be add, update or delete; got " + cmd);
      }
    }
  }

  private void handleAddDocument(DataInput in, DataOutput out) throws IOException {
    int fieldCount = in.readVInt();
    Document doc = new Document();
    for(int i=0;i<fieldCount;i++) {
      String name = in.readString();
      String value = in.readString();
      // NOTE: clearly NOT general!
      if (name.equals("docid") || name.equals("marker")) {
        doc.add(new StringField(name, value, Field.Store.YES));
      } else if (name.equals("title")) {
        doc.add(new StringField("title", value, Field.Store.YES));
        doc.add(new Field("titleTokenized", value, tokenizedWithTermVectors));
      } else if (name.equals("body")) {
        doc.add(new Field("body", value, tokenizedWithTermVectors));
      } else {
        throw new IllegalArgumentException("unhandled field name " + name);
      }
    }
    writer.addDocument(doc);
  }

  private void handleUpdateDocument(DataInput in, DataOutput out) throws IOException {
    int fieldCount = in.readVInt();
    Document doc = new Document();
    String docid = null;
    for(int i=0;i<fieldCount;i++) {
      String name = in.readString();
      String value = in.readString();
      // NOTE: clearly NOT general!
      if (name.equals("docid")) {
        docid = value;
        doc.add(new StringField("docid", value, Field.Store.YES));
      } else if (name.equals("marker")) {
        doc.add(new StringField("marker", value, Field.Store.YES));
      } else if (name.equals("title")) {
        doc.add(new StringField("title", value, Field.Store.YES));
        doc.add(new Field("titleTokenized", value, tokenizedWithTermVectors));
      } else if (name.equals("body")) {
        doc.add(new Field("body", value, tokenizedWithTermVectors));
      } else {
        throw new IllegalArgumentException("unhandled field name " + name);
      }
    }

    writer.updateDocument(new Term("docid", docid), doc);
  }

  private void handleDeleteDocument(DataInput in, DataOutput out) throws IOException {
    String docid = in.readString();
    writer.deleteDocuments(new Term("docid", docid));
  }

  // Sent to primary to cutover new SIS:
  static final byte CMD_FLUSH = 10;

  // Sent by replica to primary asking to copy a set of files over:
  static final byte CMD_FETCH_FILES = 1;
  static final byte CMD_GET_SEARCHING_VERSION = 12;
  static final byte CMD_SEARCH = 2;
  static final byte CMD_MARKER_SEARCH = 3;
  static final byte CMD_COMMIT = 4;
  static final byte CMD_CLOSE = 5;
  static final byte CMD_SEARCH_ALL = 21;

  // Send (to primary) the list of currently running replicas:
  static final byte CMD_SET_REPLICAS = 16;

  // Multiple indexing ops
  static final byte CMD_INDEXING = 18;
  static final byte CMD_ADD_DOC = 6;
  static final byte CMD_UPDATE_DOC = 7;
  static final byte CMD_DELETE_DOC = 8;
  static final byte CMD_INDEXING_DONE = 19;
  static final byte CMD_DELETE_ALL_DOCS = 22;
  static final byte CMD_FORCE_MERGE = 23;

  // Sent by replica to primary when replica first starts up, so primary can add it to any warming merges:
  static final byte CMD_NEW_REPLICA = 20;

  /** Handles incoming request to the naive TCP server wrapping this node */
  void handleOneConnection(Random random, ServerSocket ss, AtomicBoolean stop, InputStream is, Socket socket, DataInput in, DataOutput out, BufferedOutputStream bos) throws IOException, InterruptedException {

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

      switch (cmd) {

      case CMD_FLUSH:
        handleFlush(in, out, bos);
        break;

      case CMD_FETCH_FILES:
        // Replica (other node) is asking us (primary node) for files to copy
        handleFetchFiles(random, socket, in, out, bos);
        break;

      case CMD_INDEXING:
        handleIndexing(socket, stop, is, in, out, bos);
        break;

      case CMD_GET_SEARCHING_VERSION:
        out.writeVLong(getCurrentSearchingVersion());
        break;

      case CMD_SEARCH:
        {
          Thread.currentThread().setName("search");
          IndexSearcher searcher = mgr.acquire();
          try {
            long version = ((DirectoryReader) searcher.getIndexReader()).getVersion();
            int hitCount = searcher.count(new TermQuery(new Term("body", "the")));
            //message("version=" + version + " searcher=" + searcher);
            out.writeVLong(version);
            out.writeVInt(hitCount);
            bos.flush();
          } finally {
            mgr.release(searcher);
          }
          bos.flush();
        }
        continue outer;

      case CMD_SEARCH_ALL:
        {
          Thread.currentThread().setName("search all");
          IndexSearcher searcher = mgr.acquire();
          try {
            long version = ((DirectoryReader) searcher.getIndexReader()).getVersion();
            int hitCount = searcher.count(new MatchAllDocsQuery());
            //message("version=" + version + " searcher=" + searcher);
            out.writeVLong(version);
            out.writeVInt(hitCount);
            bos.flush();
          } finally {
            mgr.release(searcher);
          }
        }
        continue outer;

      case CMD_MARKER_SEARCH:
        {
          Thread.currentThread().setName("msearch");
          int expectedAtLeastCount = in.readVInt();
          verifyAtLeastMarkerCount(expectedAtLeastCount, out);
          bos.flush();
        }
        continue outer;

      case CMD_COMMIT:
        Thread.currentThread().setName("commit");
        commit();
        out.writeByte((byte) 1);
        break;

      case CMD_CLOSE:
        Thread.currentThread().setName("close");
        message("top close: now close server socket");
        ss.close();
        out.writeByte((byte) 1);
        message("top close: done close server socket");
        break;

      case CMD_SET_REPLICAS:
        Thread.currentThread().setName("set repls");
        int count = in.readVInt();
        int[] replicaIDs = new int[count];
        int[] replicaTCPPorts = new int[count];
        for(int i=0;i<count;i++) {
          replicaIDs[i] = in.readVInt();
          replicaTCPPorts[i] = in.readVInt();
        }
        out.writeByte((byte) 1);
        setReplicas(replicaIDs, replicaTCPPorts);
        break;

      case CMD_NEW_REPLICA:
        Thread.currentThread().setName("new repl");
        int replicaTCPPort = in.readVInt();
        message("new replica: " + warmingSegments.size() + " current warming merges");
        // Step through all currently warming segments and try to add this replica if it isn't there already:
        synchronized(warmingSegments) {
          for(MergePreCopy preCopy : warmingSegments) {
            message("warming segment " + preCopy.files.keySet());
            boolean found = false;
            synchronized (preCopy.connections) {
              for(Connection c : preCopy.connections) {
                if (c.destTCPPort == replicaTCPPort) {
                  found = true;
                  break;
                }
              }
            }

            if (found) {
              message("this replica is already warming this segment; skipping");
              // It's possible (maybe) that the replica started up, then a merge kicked off, and it warmed to this new replica, all before the
              // replica sent us this command:
              continue;
            }

            // OK, this new replica is not already warming this segment, so attempt (could fail) to start warming now:

            Connection c = new Connection(replicaTCPPort);
            if (preCopy.tryAddConnection(c) == false) {
              // This can happen, if all other replicas just now finished warming this segment, and so we were just a bit too late.  In this
              // case the segment will be copied over in the next nrt point sent to this replica
              message("failed to add connection to segment warmer (too late); closing");
              c.close();
            }
            c.out.writeByte(SimpleReplicaNode.CMD_PRE_COPY_MERGE);
            c.out.writeVLong(primaryGen);
            c.out.writeVInt(tcpPort);
            SimpleServer.writeFilesMetaData(c.out, preCopy.files);
            c.flush();
            c.s.shutdownOutput();
            message("successfully started warming");
          }
        }
        break;

      default:
        throw new IllegalArgumentException("unrecognized cmd=" + cmd + " via socket=" + socket);
      }
      bos.flush();
      break;
    }
  }

  private void verifyAtLeastMarkerCount(int expectedAtLeastCount, DataOutput out) throws IOException {
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
        throw new IllegalStateException("at flush: marker count " + hitCount + " but expected at least " + expectedAtLeastCount + " version=" + version);
      }

      if (out != null) {
        out.writeVLong(version);
        out.writeVInt(hitCount);
      }
    } finally {
      mgr.release(searcher);
    }
  }
}
