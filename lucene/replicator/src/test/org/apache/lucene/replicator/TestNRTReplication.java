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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.TreeLogger;
import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;

// nocommit add fang: master crashes

// nocommit what about network partitioning

// nocommit make MDW throw exceptions sometimes

// nocommit test warming merge w/ O_DIRECT via NativeUnixDir

// nocommit test rare bit errors during copy

// nocommit: master's IW must not do reader pooling?  else
// deletes are not pushed to disk?

// nocommit test slow replica

// nocommit make flush sync async

// nocommit test replica that "falls out" because it's too
// slow and then tries to join back again w/o having
// "properly" restarted

// nocommit also test sometimes starting up new master from
// a down replica (ie, not just promoting an already running
// replica)

// nocommit rewrite the test so each node has its own
// threads, to be closer to the concurrency we'd "really"
// see across N machines

// nocommit checksum_N files never delete!

@SuppressCodecs({ "SimpleText", "Memory", "Direct" })
public class TestNRTReplication extends LuceneTestCase {

  static volatile Master master;
  static ReentrantLock masterLock = new ReentrantLock();

  @AfterClass
  public static void afterClass() {
    master = null;
  }

  public void test() throws Throwable {
    try {
      _test();
    } catch (Throwable t) {
      System.out.println("FAILED:");
      t.printStackTrace(System.out);
      throw t;
    }
  }

  /*
  private static Map<String,Long> globalState = new HashMap<>();

  private static void setGlobalStateKeyVal(String key, long value) {
    Long cur = globalState.get(key);
    assert cur == null || cur <= value;
    TreeLogger.log("  push " + key + " cur=" + cur + " new=" + value);
    globalState.put(key, value);
  }
  */

  /** Called just before IndexWriter finishCommit on the
   *  current master, to push "next write gens" to global
   *  state. */
  /*
  static void pushGlobalState(SegmentInfos infos) {
    TreeLogger.log("TEST: now pushGlobalState");
    TreeLogger.start("pushGlobalState");    
    // NOTE: assumed externally sync'd, i.e. only one master
    // across the cluster at a time
    setGlobalStateKeyVal("segmentsGen", infos.getGeneration());
    // nocommit weird that we must add 2 :)
    setGlobalStateKeyVal("segmentsVersion", infos.getVersion()+2);

    // Used to generate next segment file name:
    setGlobalStateKeyVal("segmentsCounter", (long) infos.counter);

    for(SegmentCommitInfo info : infos) {
      setGlobalStateKeyVal(info.info.name + "_delGen",  info.getNextDelGen());
      setGlobalStateKeyVal(info.info.name + "_fieldInfosGen",  info.getNextFieldInfosGen());
    }
    TreeLogger.end("pushGlobalState");
  }
  */

  /** Called just before init of a new writer, to pull the
   *  "next write gens" and set them in the current infos. */
  /*
  static void pullGlobalState(SegmentInfos infos) {
    TreeLogger.log("TEST: now pullGlobalState");
    TreeLogger.start("pullGlobalState");
    Long v = globalState.get("segmentsGen");
    if (v == null) {
      TreeLogger.log("no global state yet; skip");
      TreeLogger.end("pullGlobalState");
      return;
    }
    TreeLogger.log("pull global gen=" + v + " vs cur=" + infos.getGeneration());
    assert infos.getGeneration() <= v.longValue(): "infos.generation=" + infos.getGeneration() + " global.generation=" + v;
    infos.setGeneration(v.longValue());

    v = globalState.get("segmentsVersion");
    assert v != null;
    assert infos.version <= v.longValue(): "infos.version=" + infos.version + " global.version=" + v;
    TreeLogger.log("pull global version=" + v + " vs cur=" + infos.version);
    infos.version = v.longValue();

    v = globalState.get("segmentsCounter");
    assert v != null;
    assert infos.counter <= v.longValue(): "infos.counter=" + infos.counter + " global.counter=" + v;
    TreeLogger.log("pull global counter=" + v + " vs cur=" + infos.counter);
    infos.counter = v.intValue();

    for(SegmentCommitInfo info : infos) {
      String key = info.info.name + "_delGen";
      v = globalState.get(key);
      long value = v == null ? 1 : v.longValue();
      assert info.getNextDelGen() <= value: "seg=" + info.info.name + " delGen=" + info.getNextDelGen() + " vs global=" + value;
      TreeLogger.log("pull global del gen=" + v + " for seg=" + info.info.name + " vs cur=" + info.getNextDelGen());
      info.setNextDelGen(value);

      key = info.info.name + "_fieldInfosGen";
      v = globalState.get(key);
      value = v == null ? 1 : v.longValue();
      assert info.getNextFieldInfosGen() <= value: "seg=" + info.info.name + " fieldInfosGen=" + info.getNextFieldInfosGen() + " vs global=" + value;
      TreeLogger.log("pull global fieldInfos gen= " + v + " for seg=" + info.info.name + " vs cur=" + info.getNextFieldInfosGen());
      info.setNextFieldInfosGen(value);
    }
    TreeLogger.end("pullGlobalState");
  }
  */
  
  private void _test() throws Exception {

    TreeLogger.setLogger(new TreeLogger("main"));

    // Maps all segmentInfos.getVersion() we've seen, to the
    // expected doc count.  On each replica when we do a
    // *:* search we verify the totalHits is correct:
    final Map<Long,Integer> versionDocCounts = new ConcurrentHashMap<Long,Integer>();

    int numDirs = 1+_TestUtil.nextInt(random(), 2, 4);
    // nocommit
    numDirs = 6;
    final File[] dirs = new File[numDirs];
    // One Master and N-1 Replica:
    final Object[] nodes = new Object[numDirs];
    System.out.println("TEST: " + nodes.length + " nodes");
    for(int i=0;i<numDirs;i++) {
      dirs[i] = _TestUtil.getTempDir("NRTReplication");
      if (i > 0) {
        if (random().nextInt(10) < 7) {
          // Some replicas don't start on init:
          nodes[i] = new Replica(dirs[i], i, versionDocCounts, null);
        } else {
          System.out.println("TEST: skip replica " + i + " startup");
        }
      }
    }

    nodes[0] = master = new Master(dirs[0], 0, nodes, versionDocCounts);

    // Periodically stops/starts/commits replicas, moves master:
    CommitThread commitThread = new CommitThread(dirs, nodes, versionDocCounts);
    commitThread.setName("commitThread");
    commitThread.start();

    // nocommit test graceful full shutdown / restart

    // nocommit test all nodes hard shutdown

    long endTime = System.currentTimeMillis() + 60000;
    //long endTime = System.currentTimeMillis() + 10000;

    while (System.currentTimeMillis() < endTime) {
      Thread.sleep(_TestUtil.nextInt(random(), 2, 20));

      assert master != null;

      TreeLogger.log("\nTEST: now replicate master id=" + master.id);
      TreeLogger.start("replicate");

      SegmentInfos infos;
      boolean closeMaster = false;
      if (master == null) {
        infos = null;
      } else {
        if (random().nextInt(100) == 57) {
          closeMaster = true;
          TreeLogger.log("top: id=" + master.id + " now move master");
          // Commits & closes current master and pull the
          // infos of the final commit:
          masterLock.lock();
          try {
            infos = master.close(random().nextBoolean());
          } finally {
            masterLock.unlock();
          }
          TreeLogger.log("top: done shutdown master; version=" + infos.version);
        } else {

          // Have writer do a full flush, and return the
          // resulting segments, protected from deletion
          // (incRef'd) just while we copy the files out to
          // the replica (s).  This is just like pulling an
          // NRT reader, except we don't actually open the
          // readers on the newly flushed segments:
          TreeLogger.log("flush current master");
          infos = master.writer.w.flushAndIncRef();
        }
      }

      if (infos != null) {

        int count = docCount(infos);
        TreeLogger.log("record version=" + infos.version + " count=" + count + " segs=" + infos.toString(master.dir));
        Integer oldCount = versionDocCounts.put(infos.version, count);

        // nocommit cannot do this: versions can go backwards
        //if (oldCount != null) {
        //assertEquals("version=" + infos.version + " oldCount=" + oldCount + " newCount=" + count, oldCount.intValue(), count);
        //}

        // nocommit need to do this concurrently w/ pushing
        // out to replicas:
        if (closeMaster == false) {
          master.mgr.refresh(infos);
        }

        // nocommit can we have commit commit the "older" SIS?
        // they will commit quickly since the OS will have
        // already moved those bytes to disk...

        int totDocCount = docCount(infos);
        String extra = " master.sizeInBytes=" + ((NRTCachingDirectory) master.dir.getDelegate()).sizeInBytes();

        TreeLogger.log("replicate docCount=" + totDocCount + " version=" + infos.version + extra + " segments=" + infos.toString(master.dir));

        // Convert infos to byte[], to send "on the wire":
        RAMOutputStream out = new RAMOutputStream();
        infos.write(out);
        byte[] infosBytes = new byte[(int) out.getFilePointer()];
        out.writeTo(infosBytes, 0);
        
        // nocommit test master crash (promoting replica to master)

        // nocommit do this sync in separate threads
        Map<String,FileMetaData> filesMetaData = getFilesMetaData(master, infos.files(master.dir, false));

        // nocommit simulate super-slow replica: it should not
        // hold up the copying of other replicas, nor new
        // flushing; the copy of a given SIS to a given
        // replica should be fully concurrent/bg
        int upto = 0;
        for(Object n : nodes) {
          if (n != null && n instanceof Replica) {
            Replica r = (Replica) n;

            // nocommit improve this: load each file ONCE,
            // push to the N replicas that need it
            try {
              r.sync(master.dir, filesMetaData, infosBytes, infos.version);
            } catch (AlreadyClosedException ace) {
              // Ignore this: it means the replica shut down
              // while we were trying to sync.  This
              // "approximates" an exception the master would
              // see trying to push file bytes to a replica
              // that was just taken offline.
            } catch (Exception e) {
              TreeLogger.log("TEST FAIL: replica " + r.id, e);
              throw e;
            }
          } else if (n == null) {
            TreeLogger.log("id=" + upto + " skip down replica");
          }
          upto++;
        }
      }

      if (closeMaster == false) {
        // Done pushing to all replicas so we now release
        // the files on master, so IW is free to delete if it
        // needs to:
        master.setInfos(infos);
      } else {

        // clean this up:
        TreeLogger.log("close old master dir dir.listAll()=" + Arrays.toString(master.dir.listAll()));
        master.dir.close();

        masterLock.lock();
        try {

          nodes[master.id] = null;

          // nocommit go back to picking random replica

          // Must pick newest replica to promote, else we
          // can't overwrite open files when trying to copy
          // to the newer replicas:
          int bestIDX = -1;
          long highestVersion = -1;
          for(int idx=0;idx<nodes.length;idx++) {
            if (nodes[idx] instanceof Replica) {
              Replica r = (Replica) nodes[idx];
              long version = r.mgr.getCurrentInfos().version;
              if (version > highestVersion) {
                bestIDX = idx;
                highestVersion = version;
              }
            }
          }

          int idx;
          if (bestIDX != -1) {
            idx = bestIDX;
          } else {
            // All replicas are down; it doesn't matter
            // which one we pick
            idx = random().nextInt(nodes.length);
          }

          if (nodes[idx] == null) {
            // Start up Master from scratch:
            TreeLogger.log("top: id=" + idx + " promote down node to master");
            nodes[idx] = master = new Master(dirs[idx], idx, nodes, versionDocCounts);
          } else {
            // Promote a running replica to Master:
            assert nodes[idx] instanceof Replica;
            TreeLogger.log("top: id=" + idx + " promote replica to master");
            master = new Master((Replica) nodes[idx], nodes);
            nodes[idx] = master;
          }
        } finally {
          masterLock.unlock();
        }
      }
      TreeLogger.end("replicate");
    }

    System.out.println("TEST: stop commit thread");
    commitThread.finish();

    if (master != null) {
      System.out.println("TEST: close master");
      masterLock.lock();
      try {
        master.close(random().nextBoolean());
        master.dir.close();
      } finally {
        masterLock.unlock();
      }
    }

    System.out.println("TEST: close replicas");
    for(Object n : nodes) { 
      if (n != null && n instanceof Replica) {
        ((Replica) n).shutdown();
      }
    }
  }

  static class FileMetaData {
    public final long sizeInBytes;
    public final long checksum;

    public FileMetaData(long sizeInBytes, long checksum) {
      this.sizeInBytes = sizeInBytes;
      this.checksum = checksum;
    }
  }

  static Map<String,FileMetaData> getFilesMetaData(Master master, Collection<String> files) throws IOException {
    Map<String,FileMetaData> filesMetaData = new HashMap<String,FileMetaData>();
    for(String file : files) {
      Long checksum = master.dir.getChecksum(file);
      assert checksum != null;
      filesMetaData.put(file, new FileMetaData(master.dir.fileLength(file), checksum.longValue()));
    }

    return filesMetaData;
  }

  static Set<String> copyFiles(SlowChecksumDirectory src, Replica dst, Map<String,FileMetaData> filesMetaData, boolean lowPriority) throws IOException {
    long t0 = System.currentTimeMillis();
    long totBytes = 0;
    Set<String> toCopy = new HashSet<String>();
    for(Map.Entry<String,FileMetaData> ent : filesMetaData.entrySet()) {
      String fileName = ent.getKey();
      // nocommit remove now unused metaData.checksum
      FileMetaData metaData = ent.getValue();
      Long srcChecksum0 = src.getChecksum(fileName);
      assert srcChecksum0 != null: "id=" + dst.id + " name=" + fileName;
      long srcChecksum = srcChecksum0.longValue();

      Long checksum = dst.dir.getChecksum(fileName);
      if (dst.dir.fileExists(fileName) == false) {
        TreeLogger.log("id=" + dst.id + " " + fileName + " will copy [does not exist] length=" + metaData.sizeInBytes + " srcChecksum=" + srcChecksum);
        toCopy.add(fileName);
        totBytes += metaData.sizeInBytes;
      } else if (dst.dir.fileLength(fileName) != metaData.sizeInBytes) {
        TreeLogger.log("id=" + dst.id + " " + fileName + " will copy [different file length old=" + dst.dir.fileLength(fileName) + " new=" + metaData.sizeInBytes + "]");
        toCopy.add(fileName);
        totBytes += metaData.sizeInBytes;
      } else if (checksum == null) {
        TreeLogger.log("id=" + dst.id + " " + fileName + " will copy [no checksum] length=" + metaData.sizeInBytes);
        toCopy.add(fileName);
        totBytes += metaData.sizeInBytes;
      } else if (checksum.longValue() != srcChecksum) {
        TreeLogger.log("id=" + dst.id + " " + fileName + " will copy [wrong checksum: cur=" + checksum.longValue() + " new=" + srcChecksum + "] length=" + metaData.sizeInBytes);
        toCopy.add(fileName);
        totBytes += metaData.sizeInBytes;
      } else {
        TreeLogger.log("id=" + dst.id + " " + fileName + " skip copy checksum=" + srcChecksum + " file.length=" + metaData.sizeInBytes);
      }
    }

    // nocommit should we "organize" the files to be copied
    // by segment name?  so that NRTCachingDir can
    // separately decide on a seg by seg basis whether to
    // cache the seg's files?
    IOContext ioContext;
    if (lowPriority) {
      // nocommit do we need the tot docs / merge num segs?
      ioContext = new IOContext(new MergeInfo(0, totBytes, false, 0));
    } else {
      // nocommit can we get numDocs?
      ioContext = new IOContext(new FlushInfo(0, totBytes));
    }
    for(String f : toCopy) {
      long bytes = src.fileLength(f);
      //System.out.println("  copy " + f + " (" + bytes + " bytes)");
      totBytes += bytes;
      TreeLogger.log("id=" + dst.id + " " + f + " copy file");

      dst.copyOneFile(dst.id, src, f, ioContext);

      // nocommit make test that exercises this
      // Make sure no bits flipped during copy
      Long v1 = dst.dir.getChecksum(f);
      assert v1 != null;

      Long v2 = src.getChecksum(f);
      assert v2 != null;

      if (v1.longValue() != v2.longValue()) {
        throw new IOException("id=" + dst.id + " " + f + ": copy failed: wrong checksums src=" + v2 + " vs dst=" + v1);
      }

      if (lowPriority) {
        // Rate limit low priority (copying a merged segment):
        // nocommit use rate limiter
        try {
          Thread.sleep(bytes/100000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(ie);
        }
      }
    }
    long t1 = System.currentTimeMillis();
    TreeLogger.log("replica " + dst.id + ": " + (lowPriority ? "low-priority " : "") + "took " + (t1-t0) + " millis for " + totBytes + " bytes; " + toCopy.size() + " files (of " + filesMetaData.size() + "); sizeInBytes=" + ((NRTCachingDirectory) dst.dir.getDelegate()).sizeInBytes());

    return toCopy;
  }

  /** Like SearcherManager, except it refreshes via a
   *  provided (NRT) SegmentInfos. */
  private static class InfosSearcherManager extends ReferenceManager<IndexSearcher> {
    private SegmentInfos currentInfos;
    private final Directory dir;

    public InfosSearcherManager(Directory dir, int id, SegmentInfos infosIn) throws IOException {
      this.dir = dir;
      if (infosIn != null) {
        currentInfos = infosIn;
        TreeLogger.log("InfosSearcherManager.init id=" + id + ": use incoming infos=" + infosIn.toString(dir));
      } else {
        currentInfos = new SegmentInfos();
        String fileName = SegmentInfos.getLastCommitSegmentsFileName(dir);
        if (fileName != null) {
          // Load last commit:
          currentInfos.read(dir, fileName);
          TreeLogger.log("InfosSearcherManager.init id=" + id + ": load initial segments file " + fileName + ": loaded version=" + currentInfos.version + " docCount=" + docCount(currentInfos));
        }
      }
      current = new IndexSearcher(StandardDirectoryReader.open(dir, currentInfos, null));
    }

    @Override
    protected int getRefCount(IndexSearcher s) {
      return s.getIndexReader().getRefCount();
    }

    @Override
    protected boolean tryIncRef(IndexSearcher s) {
      return s.getIndexReader().tryIncRef();
    }

    @Override
    protected void decRef(IndexSearcher s) throws IOException {
      s.getIndexReader().decRef();
    }

    public SegmentInfos getCurrentInfos() {
      return currentInfos;
    }
    
    public void refresh(SegmentInfos infos) throws IOException {
      if (currentInfos != null) {
        // So that if we commit, we will go to the next
        // (unwritten so far) generation:
        infos.updateGeneration(currentInfos);
      }
      currentInfos = infos;
      maybeRefresh();
    }

    @Override
    protected IndexSearcher refreshIfNeeded(IndexSearcher old) throws IOException {
      List<AtomicReader> subs;
      if (old == null) {
        subs = null;
      } else {
        subs = new ArrayList<AtomicReader>();
        for(AtomicReaderContext ctx : old.getIndexReader().leaves()) {
          subs.add(ctx.reader());
        }
      }

      return new IndexSearcher(StandardDirectoryReader.open(dir, currentInfos, subs));
    }
  }

  static class CommitThread extends Thread {
    private final File[] dirs;
    private final Object[] nodes;
    private final Map<Long,Integer> versionDocCounts;
    private volatile boolean stop;

    public CommitThread(File[] dirs, Object[] nodes, Map<Long,Integer> versionDocCounts) {
      this.dirs = dirs;
      this.nodes = nodes;
      this.versionDocCounts = versionDocCounts;
    }

    @Override
    public void run() {
      TreeLogger.setLogger(new TreeLogger("commit"));
      try {
        while (stop == false) {
          Thread.sleep(_TestUtil.nextInt(random(), 10, 30));
          int i = random().nextInt(nodes.length);

          masterLock.lock();
          try {
            Object n = nodes[i];
            if (n != null) {
              if (n instanceof Replica) {
                Replica r = (Replica) n;
                if (random().nextInt(100) == 17) {
                  TreeLogger.log("top: id=" + i + " commit");
                  r.commit(false);
                }
                if (random().nextInt(100) == 17) {
                  // Shutdown this replica
                  nodes[i] = null;
                  TreeLogger.log("top: id=" + i + " shutdown replica");
                  r.shutdown();
                } else if (random().nextInt(100) == 17) {
                  // Crash the replica
                  nodes[i] = null;
                  TreeLogger.log("top: id=" + i + " crash replica");
                  r.crash();
                }
              } else if (master != null && master.isClosed() == false) {
                // Randomly commit master:
                if (random().nextInt(100) == 17) {
                  TreeLogger.log("top: id=" + i + " commit master");
                  master.commit();
                }
              }
            } else if (random().nextInt(20) == 17) {
              // Restart this replica
              try {
                nodes[i] = new Replica(dirs[i], i, versionDocCounts, null);
              } catch (Throwable t) {
                TreeLogger.log("top: id=" + i + " FAIL startup", t);
                throw t;
              }
            }
          } finally {
            masterLock.unlock();
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void finish() throws InterruptedException {
      stop = true;
      join();
    }
  }

  // nocommit should extend replica and just add writer
  private static class Master {

    final Set<String> finishedMergedSegments = Collections.newSetFromMap(new ConcurrentHashMap<String,Boolean>());

    final SlowChecksumDirectory dir;
    final Object[] nodes;

    final RandomIndexWriter writer;
    final InfosSearcherManager mgr;
    final SearchThread searchThread;
    final IndexThread indexThread;
    final int id;
    private boolean isClosed;

    SegmentInfos lastInfos;

    /** Start up a master from scratch. */
    public Master(File path,
                  int id, Object[] nodes, Map<Long,Integer> versionDocCounts) throws IOException {
      final MockDirectoryWrapper dirOrig = newMockFSDirectory(path);

      // In some legitimate cases we will double-write:
      dirOrig.setPreventDoubleWrite(false);

      this.id = id;
      this.nodes = nodes;

      // nocommit put back
      dirOrig.setCheckIndexOnClose(false);

      dir = new SlowChecksumDirectory(id, new NRTCachingDirectory(dirOrig, 1.0, 10.0));
      //((NRTCachingDirectory) master).VERBOSE = true;
      SegmentInfos infos = new SegmentInfos();
      try {
        infos.read(dir);
      } catch (IndexNotFoundException infe) {
      }

      //pullGlobalState(infos);

      mgr = new InfosSearcherManager(dir, id, infos);
      searchThread = new SearchThread("master", mgr, versionDocCounts);
      searchThread.start();

      SegmentInfos curInfos = mgr.getCurrentInfos().clone();

      IndexWriterConfig iwc = getIWC();
      iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);

      // nocommit also test periodically committing, and
      // preserving multiple commit points; verify these
      // "survive" over to the replica
      writer = new RandomIndexWriter(random(), dir, curInfos, iwc);
      _TestUtil.reduceOpenFiles(writer.w);
      System.out.println("after reduce: " + writer.w.getConfig());

      lastInfos = mgr.getCurrentInfos();
      // nocommit thread hazard here?  IW could have already
      // nuked some segments...?
      writer.w.incRefDeleter(lastInfos);

      indexThread = new IndexThread(this);
      indexThread.start();
    }

    public void commit() throws IOException {
      writer.w.prepareCommit();
      //pushGlobalState(writer.w.getPendingCommit());
      // It's harmless if we crash here, because the
      // global state has already been updated
      writer.w.commit();
    }

    /** Promotes an existing Replica to Master, re-using the
     *  open NRTCachingDir, the SearcherManager, the search
     *  thread, etc. */
    public Master(Replica replica, Object[] nodes) throws IOException {
      this.id = replica.id;
      this.dir = replica.dir;
      this.nodes = nodes;
      this.mgr = replica.mgr;
      this.searchThread = replica.searchThread;

      // nocommit must somehow "stop" this replica?  e.g. we
      // don't want it doing any more deleting?

      IndexWriterConfig iwc = getIWC();
      iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);

      // nocommit also test periodically committing, and
      // preserving multiple commit points; verify these
      // "survive" over to the replica

      SegmentInfos curInfos = replica.mgr.getCurrentInfos().clone();
      //pullGlobalState(curInfos);

      writer = new RandomIndexWriter(random(), dir, curInfos, iwc);
      _TestUtil.reduceOpenFiles(writer.w);
      System.out.println("after reduce: " + writer.w.getConfig());

      lastInfos = mgr.getCurrentInfos();
      // nocommit thread hazard here?  IW could have already
      // nuked some segments...?
      writer.w.incRefDeleter(lastInfos);

      indexThread = new IndexThread(this);
      indexThread.start();
    }

    private IndexWriterConfig getIWC() {
      IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));

      // Install a merged segment warmer that pre-copies (low
      // priority) merged segment files out to replica(s)
      // before the merge is swapped into the live segments;
      // this way a large merge won't interfere with
      // NRT turnaround time:
      iwc.setMergedSegmentWarmer(new IndexReaderWarmer() {
          @Override
          public void warm(AtomicReader reader) throws IOException {
            SegmentCommitInfo info = ((SegmentReader) reader).getSegmentInfo();
            if (TreeLogger.getLogger() == null) {
              TreeLogger.setLogger(new TreeLogger("merge"));
            }
            //System.out.println("TEST: warm merged segment files " + info);
            Map<String,FileMetaData> filesMetaData = getFilesMetaData(Master.this, info.files());
            for(Object n : nodes) {
              if (n != null && n instanceof Replica) {
                try {
                  // nocommit do we need to check for merge aborted...?
                  ((Replica) n).warmMerge(info.info.name, dir, filesMetaData);
                } catch (AlreadyClosedException ace) {
                  // Ignore this: it means the replica shut down
                  // while we were trying to copy files.  This
                  // "approximates" an exception the master would
                  // see trying to push file bytes to a replica
                  // that was just taken offline.
                }
              }
            }
          }
        });

      return iwc;
    }

    /** Gracefully shuts down the master, and returns the
     *  final segments in the index .*/
    public SegmentInfos close(boolean waitForMerges) throws IOException {
      TreeLogger.log("id=" + id + " close master waitForMerges=" + waitForMerges);

      try {
        searchThread.finish();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ie);
      }

      mgr.close();

      if (waitForMerges) {
        // Do it here, instead of on close, so we can
        // continue indexing while waiting for merges:
        writer.w.waitForMerges();
        TreeLogger.log("waitForMerges done");
      }

      if (lastInfos != null) {
        writer.w.decRefDeleter(lastInfos);
        lastInfos = null;
      }

      try {
        indexThread.finish();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ie);
      }

      TreeLogger.log("close writer");

      // nocommit can we optionally NOT commit here?  it
      // should be decoupled from master migration?

      commit();

      // Don't wait for merges now; we already did above:
      writer.close(false);
      TreeLogger.log("done close writer");

      SegmentInfos infos = new SegmentInfos();
      infos.read(dir);

      TreeLogger.log("final infos=" + infos.toString(master.dir));

      // nocommit caller must close
      // dir.close();
      isClosed = true;

      return infos;
    }

    public synchronized boolean isClosed() {
      return isClosed;
    }

    public synchronized SegmentInfos getInfos() throws IOException {
      writer.w.incRefDeleter(lastInfos);
      return lastInfos;
    }

    // NOTE: steals incoming ref
    public synchronized void setInfos(SegmentInfos newInfos) throws IOException {
      writer.w.decRefDeleter(lastInfos);
      lastInfos = newInfos;
    }

    public synchronized void releaseInfos(SegmentInfos infos) throws IOException {
      writer.w.decRefDeleter(infos);
    }
  }

  static class IndexThread extends Thread {

    final Master master;
    volatile boolean stop;

    public IndexThread(Master master) {
      this.master = master;
      setName("IndexThread id=" + master.id);
    }

    @Override
    public void run() {
      try {
        int docID = 0;
        LineFileDocs docs = new LineFileDocs(random());
        while (stop == false) {
          Document doc = docs.nextDoc();
          Field idField = doc.getField("docid");
          if (random().nextInt(10) == 9 && docID > 0) {
            int randomDocID = random().nextInt(docID);
            idField.setStringValue(""+randomDocID);
            master.writer.updateDocument(new Term("docid", ""+randomDocID), doc);
          } else {
            idField.setStringValue(""+docID);
            master.writer.addDocument(doc);
            docID++;
          }
          // ~500 docs/sec
          Thread.sleep(2);
        }
      } catch (Exception e) {
        throw new RuntimeException("IndexThread on id=" + master.id + ": " + e, e);
      }
    }

    public void finish() throws InterruptedException {
      stop = true;
      join();
    }
  };

  private static class Replica {
    final int id;
    final SlowChecksumDirectory dir;
    final InfosRefCounts deleter;

    private final InfosSearcherManager mgr;
    private volatile boolean stop;
    private SearchThread searchThread;

    private final Collection<String> lastCommitFiles;
    private final Collection<String> lastNRTFiles;

    // nocommit unused so far:
    private final IndexReaderWarmer mergedSegmentWarmer;

    public Replica(File path, int id, Map<Long,Integer> versionDocCounts, IndexReaderWarmer mergedSegmentWarmer) throws IOException {
      TreeLogger.log("top: id=" + id + " replica startup path=" + path);
      TreeLogger.start("startup");
      
      this.id = id;
      this.mergedSegmentWarmer = mergedSegmentWarmer;

      Directory fsDir = newMockFSDirectory(path);

      // In some legitimate cases we will double-write:
      ((MockDirectoryWrapper) fsDir).setPreventDoubleWrite(false);
      
      // nocommit put back
      ((BaseDirectoryWrapper) fsDir).setCheckIndexOnClose(false);

      dir = new SlowChecksumDirectory(id, new NRTCachingDirectory(fsDir, 1.0, 10.0));
      TreeLogger.log("id=" + id + " created dirs, checksums; dir.listAll=" + Arrays.toString(dir.listAll()));

      // nocommit don't need copiedFiles anymore:
      // Startup sync to pull latest index over:
      Set<String> copiedFiles = null;

      lastCommitFiles = new HashSet<String>();
      lastNRTFiles = new HashSet<String>();

      String segmentsFileName = SegmentInfos.getLastCommitSegmentsFileName(dir);
      if (segmentsFileName != null) {
        SegmentInfos lastCommit = new SegmentInfos();
        TreeLogger.log("id=" + id + " " + segmentsFileName + " now load");
        lastCommit.read(dir, segmentsFileName);
        lastCommitFiles.addAll(lastCommit.files(dir, true));
        TreeLogger.log("id=" + id + " lastCommitFiles = " + lastCommitFiles);
      }

      // Must sync latest index from master before starting
      // up mgr, so that we don't hold open any files that
      // need to be overwritten when the master is against
      // an older index than our copy:
      SegmentInfos infos = null;
      assert master == null || masterLock.isLocked();
      if (master != null && master.isClosed() == false) {
        SegmentInfos masterInfos = null;
        try {
          masterInfos = master.getInfos();
          // Convert infos to byte[], to send "on the wire":
          RAMOutputStream out = new RAMOutputStream();
          masterInfos.write(out);
          byte[] infosBytes = new byte[(int) out.getFilePointer()];
          out.writeTo(infosBytes, 0);

          Map<String,FileMetaData> filesMetaData = getFilesMetaData(master, masterInfos.files(master.dir, false));

          try {
            // Copy files over to replica:
            copiedFiles = copyFiles(master.dir, this, filesMetaData, false);
          } catch (Throwable t) {
            TreeLogger.log("id=" + id + " FAIL", t);
            throw new RuntimeException(t);
          }
      
          // Turn byte[] back to SegmentInfos:
          infos = new SegmentInfos();
          infos.read(dir, new ByteArrayDataInput(infosBytes));
          lastNRTFiles.addAll(infos.files(dir, false));
        } finally {
          if (masterInfos != null) {
            master.releaseInfos(masterInfos);
          }
        }
      } else {
        TreeLogger.log("id=" + id + " no master on startup; fallback to last commit");
      }

      if (copiedFiles != null) {
        // nocommit factor out & share this invalidation

        // nocommit we need to do this invalidation BEFORE
        // actually ovewriting the file?  because if we crash
        // in between the two, we've left an invalid commit?

        // If any of the files we just copied over
        // were referenced by the last commit,
        // then we must remove this commit point (it is now
        // corrupt):
        for(String fileName : copiedFiles) {
          if (lastCommitFiles.contains(fileName)) {
            TreeLogger.log("id=" + id + " " + segmentsFileName + " delete now corrupt commit and clear lastCommitFiles");
            dir.deleteFile(segmentsFileName);
            dir.deleteFile("segments.gen");
            lastCommitFiles.clear();
            break;
          }
        }
      }

      mgr = new InfosSearcherManager(dir, id, infos);

      deleter = new InfosRefCounts(id, dir);
      TreeLogger.log("id=" + id + " incRef lastCommitFiles");
      deleter.incRef(lastCommitFiles);
      TreeLogger.log("id=" + id + ": incRef lastNRTFiles=" + lastNRTFiles);
      deleter.incRef(lastNRTFiles);
      deleter.deleteUnknownFiles();

      searchThread = new SearchThread(""+id, mgr, versionDocCounts);
      searchThread.start();
      pruneChecksums();
      TreeLogger.log("done startup");
      TreeLogger.end("startup");
    }

    private void pruneChecksums() {
      TreeLogger.log("id=" + id + " now pruneChecksums");
      TreeLogger.start("pruneChecksums");
      Set<String> validFiles = new HashSet<String>();
      validFiles.addAll(lastNRTFiles);
      validFiles.addAll(lastCommitFiles);
      TreeLogger.end("pruneChecksums");
    }

    public void copyOneFile(int id, Directory src, String fileName, IOContext context) throws IOException {
      try {
        src.copy(dir, fileName, fileName, context);
      } catch (IOException ioe) {
        TreeLogger.log("id=" + id + " " + fileName + " failed copy1", ioe);
        throw ioe;
      }
    }

    // nocommit move this to a thread so N replicas copy at
    // once:

    public synchronized Set<String> sync(SlowChecksumDirectory master, Map<String,FileMetaData> filesMetaData, byte[] infosBytes,
                                         long infosVersion) throws IOException {

      if (stop) {
        throw new AlreadyClosedException("replica closed");
      }

      SegmentInfos currentInfos = mgr.getCurrentInfos();
      TreeLogger.log("id=" + id + " sync version=" + infosVersion + " vs current version=" + currentInfos.getVersion());
      TreeLogger.start("sync");

      /*
      if (currentInfos != null && currentInfos.getVersion() >= infosVersion) {
        System.out.println("  replica id=" + id + ": skip sync current version=" + currentInfos.getVersion() + " vs new version=" + infosVersion);
        return;
      }
      */

      // Copy files over to replica:
      Set<String> copiedFiles = copyFiles(master, this, filesMetaData, false);

      // Turn byte[] back to SegmentInfos:
      SegmentInfos infos = new SegmentInfos();
      infos.read(dir, new ByteArrayDataInput(infosBytes));
      TreeLogger.log("id=" + id + " replica sync version=" + infos.version + " segments=" + infos.toString(dir));

      // Delete now un-referenced files:
      Collection<String> newFiles = infos.files(dir, false);
      deleter.incRef(newFiles);
      deleter.decRef(lastNRTFiles);
      lastNRTFiles.clear();
      lastNRTFiles.addAll(newFiles);

      // nocommit factor out & share this invalidation

      // nocommit we need to do this invalidation BEFORE
      // actually ovewriting the file?  because if we crash
      // in between the two, we've left an invalid commit?

      // Invalidate the current commit if we overwrote any
      // files from it:
      for(String fileName : copiedFiles) {
        if (lastCommitFiles.contains(fileName)) {
          TreeLogger.log("id=" + id + " delete now corrupt commit and clear lastCommitFiles=" + lastCommitFiles);
          deleter.decRef(lastCommitFiles);
          dir.deleteFile("segments.gen");
          lastCommitFiles.clear();
          break;
        }
      }
      
      // Cutover to new searcher
      mgr.refresh(infos);

      TreeLogger.end("sync");
      return copiedFiles;
    }

    public synchronized void warmMerge(String segmentName, SlowChecksumDirectory master, Map<String,FileMetaData> filesMetaData) throws IOException {
      if (stop) {
        throw new AlreadyClosedException("replica closed");
      }
      TreeLogger.log("id=" + id + " replica warm merge " + segmentName);
      Set<String> copiedFiles = copyFiles(master, this, filesMetaData, true);

      // nocommit factor out & share this invalidation

      // nocommit we need to do this invalidation BEFORE
      // actually ovewriting the file?  because if we crash
      // in between the two, we've left an invalid commit?

      // Invalidate the current commit if we overwrote any
      // files from it:
      for(String fileName : copiedFiles) {
        if (lastCommitFiles.contains(fileName)) {
          TreeLogger.log("id=" + id + " delete now corrupt commit and clear lastCommitFiles=" + lastCommitFiles);
          deleter.decRef(lastCommitFiles);
          lastCommitFiles.clear();
          dir.deleteFile("segments.gen");
          break;
        }
      }

      // nocommit we could also pre-warm a SegmentReader
      // here, and add it onto subReader list for next reopen ...?
    }

    /** Gracefully close & shutdown this replica. */
    public synchronized void shutdown() throws IOException, InterruptedException {
      stop = true;
      // Sometimes shutdown w/o commiting
      TreeLogger.log("id=" + id + " replica shutdown");
      TreeLogger.start("shutdown");
      if (random().nextBoolean()) {
        TreeLogger.log("id=" + id + " commit before shutdown");
        commit(true);
      }

      searchThread.finish();
      mgr.close();

      // Delete now un-referenced files:
      TreeLogger.log("id=" + id + " now deletePending during shutdown");
      deleter.decRef(lastNRTFiles);
      lastNRTFiles.clear();
      deleter.deletePending();
      TreeLogger.log("id=" + id + " at shutdown dir.listAll()=" + Arrays.toString(dir.listAll()));
      dir.close();
      TreeLogger.end("shutdown");
    }

    /** Crashes the underlying directory, corrupting any
     *  un-sync'd files. */
    public synchronized void crash() throws IOException, InterruptedException {
      stop = true;
      TreeLogger.log("id=" + id + " replica crash; dir.listAll()=" + Arrays.toString(dir.listAll()));
      TreeLogger.log("id=" + id + " replica crash; fsdir.listAll()=" + Arrays.toString(((NRTCachingDirectory) dir.getDelegate()).getDelegate().listAll()));
      searchThread.finish();
      mgr.close();
      ((MockDirectoryWrapper) ((NRTCachingDirectory) dir.getDelegate()).getDelegate()).crash();
      ((NRTCachingDirectory) dir.getDelegate()).getDelegate().close();
    }

    /** Commit latest SegmentInfos (fsync'ing all referenced
     *  files). */
    public synchronized void commit(boolean deleteAll) throws IOException {
      SegmentInfos infos = mgr.getCurrentInfos();
      if (infos != null) {
        TreeLogger.log("id=" + id + " commit deleteAll=" + deleteAll + "; infos.version=" + infos.getVersion() + " files=" + infos.files(dir, false) + " segs=" + infos.toString(dir));
        TreeLogger.start("commit");
        Set<String> fileNames = new HashSet<String>(infos.files(dir, false));
        dir.sync(fileNames);
        // Can only save those files that have been
        // explicitly sync'd; a warmed, but not yet visible,
        // merge cannot be sync'd:
        infos.commit(dir);
        TreeLogger.log("id=" + id + " " + infos.getSegmentsFileName() + " committed");

        Collection<String> newFiles = infos.files(dir, true);
        deleter.incRef(newFiles);
        deleter.decRef(lastCommitFiles);
        lastCommitFiles.clear();
        lastCommitFiles.addAll(newFiles);
        TreeLogger.log("id=" + id + " " + infos.getSegmentsFileName() + " lastCommitFiles=" + lastCommitFiles);

        if (deleteAll) {
          // nocommit this is messy: we may delete a merge's files
          // that just copied over as we closed the writer:
          TreeLogger.log("id=" + id + " now deleteUnknownFiles during commit");
          deleter.deleteUnknownFiles();
        }

        pruneChecksums();
        TreeLogger.end("commit");
      }
    }
  }

  static class SearchThread extends Thread {
    private volatile boolean stop;
    private final InfosSearcherManager mgr;
    private final Map<Long,Integer> versionDocCounts;
    private final String id;

    public SearchThread(String id, InfosSearcherManager mgr, Map<Long,Integer> versionDocCounts) {
      this.id = id;
      this.mgr = mgr;
      this.versionDocCounts = versionDocCounts;
      setName("SearchThread id=" + id);
    }

    @Override
    public void run() {
      try {
        while (stop == false) {
          IndexSearcher s = mgr.acquire();
          try {
            // Sleep so that sometimes our searcher is "stale":
            Thread.sleep(_TestUtil.nextInt(random(), 1, 10));
            // nocommit do more interesting searches
            int totalHits = s.search(new MatchAllDocsQuery(), 10).totalHits;
            if (totalHits > 0) {
              long version = ((DirectoryReader) s.getIndexReader()).getVersion();
              Integer expectedCount = versionDocCounts.get(version);
              assertNotNull("searcher " + s + " is missing expected count", expectedCount);
              // nocommit since master may roll back in time
              // we cannot assert this:
              //assertEquals("searcher version=" + version + " replica id=" + id + " searcher=" + s, expectedCount.intValue(), totalHits);
            }
          } finally {
            mgr.release(s);
          }
        }
      } catch (Exception e) {
        System.out.println("FAILED: id=" + id);
        e.printStackTrace(System.out);
        throw new RuntimeException(e);
      }
    }

    public void finish() throws InterruptedException {
      stop = true;
      join();
    }
  }

  static int docCount(SegmentInfos infos) {
    int totDocCount = 0;
    for(SegmentCommitInfo info : infos) {
      totDocCount += info.info.getDocCount() - info.getDelCount();
    }
    return totDocCount;
  }

  // nocommit factor/share with IFD
  static class InfosRefCounts {
    private final Map<String,Integer> refCounts = new HashMap<String,Integer>();
    private final Set<String> pending = new HashSet<String>();
    private final Directory dir;
    private final int id;

    public InfosRefCounts(int id, Directory dir) throws IOException {
      this.dir = dir;
      this.id = id;
    }

    public synchronized void incRef(Collection<String> fileNames) {
      for(String fileName : fileNames) {
        Integer curCount = refCounts.get(fileName);
        if (curCount == null) {
          refCounts.put(fileName, 1);
        } else {
          refCounts.put(fileName, curCount.intValue() + 1);
        }
      }
    }

    public synchronized void decRef(Collection<String> fileNames) {
      for(String fileName : fileNames) {
        Integer curCount = refCounts.get(fileName);
        assert curCount != null;
        assert curCount.intValue() > 0;
        if (curCount.intValue() == 1) {
          refCounts.remove(fileName);
          delete(fileName);
        } else {
          refCounts.put(fileName, curCount.intValue() - 1);
        }
      }
    }

    private synchronized void delete(String fileName) {
      try {
        TreeLogger.log("id=" + id + " " + fileName + " now delete");
        dir.deleteFile(fileName);
      } catch (IOException ioe) {
        // nocommit why do we keep trying to delete the file
        // "segments" ...
        TreeLogger.log("id=" + id + " " + fileName + " delete failed; will retry later");
        pending.add(fileName);
      }
    }

    public synchronized void deletePending() {
      Set<String> copy = new HashSet<String>(pending);
      pending.clear();
      for(String fileName : copy) {
        delete(fileName);
      }
    }

    public synchronized void deleteUnknownFiles() throws IOException {
      for(String fileName : dir.listAll()) {
        if (refCounts.containsKey(fileName) == false &&
            fileName.startsWith("segments") == false &&
            fileName.startsWith("checksum") == false) {
          TreeLogger.log("id=" + id + " delete unknown file \"" + fileName + "\"");
          delete(fileName);
        }
      }
    }
  }

}
