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
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;

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

@SuppressCodecs({ "SimpleText", "Memory", "Direct" })
public class TestNRTReplication extends LuceneTestCase {

  static volatile Master master;
  static Lock masterLock = new ReentrantLock();

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

  private void _test() throws Exception {

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

    nodes[0] = master = new Master(-1, -1, dirs[0], 0, nodes, versionDocCounts);

    // Periodically stops/starts/commits replicas, moves master:
    CommitThread commitThread = new CommitThread(dirs, nodes, versionDocCounts);
    commitThread.start();

    // nocommit test graceful full shutdown / restart

    // nocommit test all nodes hard shutdown

    long endTime = System.currentTimeMillis() + 60000;
    //long endTime = System.currentTimeMillis() + 10000;

    // nocommit this is GLOBAL state; e.g. we will need
    // ZooKeeper or something to make sure this is
    // available to all nodes at all times even after crash
    // recovery:
    long globalMaxVersion = -1;
    int globalMaxSegment = -1;

    while (System.currentTimeMillis() < endTime) {
      Thread.sleep(_TestUtil.nextInt(random(), 2, 20));

      SegmentInfos infos;
      Directory masterDir;
      if (master == null) {
        infos = null;
        masterDir = null;
      } else {
        masterDir = master.dir;
        if (random().nextInt(100) == 57) {
          System.out.println("\nTEST: move master");
          // Commits & closes current master and pull the
          // infos of the final commit:
          masterLock.lock();
          try {
            infos = master.close(random().nextBoolean());
            nodes[master.id] = null;
            master = null;
          } finally {
            masterLock.unlock();
          }
          System.out.println("\nTEST: done shutdown master");
        } else {

          // Have writer do a full flush, and return the
          // resulting segments, protected from deletion
          // (incRef'd) just while we copy the files out to
          // the replica (s).  This is just like pulling an
          // NRT reader, except we don't actually open the
          // readers on the newly flushed segments:
          System.out.println("\nTEST: flushAndIncRef to replicate");
          infos = master.writer.w.flushAndIncRef();
        }
      }

      if (infos != null) {

        globalMaxVersion = infos.version;
        globalMaxSegment = infos.counter;

        int count = docCount(infos);
        System.out.println("TEST: add version=" + infos.version + " count=" + count);
        Integer oldCount = versionDocCounts.put(infos.version, count);

        if (oldCount != null) {
          assertEquals("version=" + infos.version + " oldCount=" + oldCount + " newCount=" + count, oldCount.intValue(), count);
        }

        // nocommit need to do this concurrently w/ pushing
        // out to replicas:
        if (master != null) {
          master.mgr.refresh(infos);
        }

        // nocommit can we have commit commit the "older" SIS?
        // they will commit quickly since the OS will have
        // already moved those bytes to disk...

        int totDocCount = docCount(infos);
        String extra;
        if (master != null) {
          extra = " master.sizeInBytes=" + ((NRTCachingDirectory) master.dir).sizeInBytes();
        } else {
          extra = "";
        }
        System.out.println("\nTEST: replicate docCount=" + totDocCount + " version=" + infos.version + extra + " segments=" + infos.toString(masterDir));

        // Convert infos to byte[], to send "on the wire":
        RAMOutputStream out = new RAMOutputStream();
        infos.write(out);
        byte[] infosBytes = new byte[(int) out.getFilePointer()];
        out.writeTo(infosBytes, 0);
        
        // nocommit test master crash (promoting replica to master)

        Map<String,Long> filesAndSizes = getFilesAndSizes(masterDir, infos.files(masterDir, false));
        // nocommit do this sync in separate threads

        // nocommit simulate super-slow replica: it should not
        // hold up the copying of other replicas, nor new
        // flushing; the copy of a given SIS to a given
        // replica should be fully concurrent/bg

        for(Object n : nodes) {
          if (n != null && n instanceof Replica) {
            Replica r = (Replica) n;
            // nocommit improve this: load each file ONCE,
            // push to the N replicas that need it
            try {
              r.sync(masterDir, filesAndSizes, infosBytes, infos.version);
            } catch (AlreadyClosedException ace) {
              // Ignore this: it means the replica shut down
              // while we were trying to sync.  This
              // "approximates" an exception the master would
              // see trying to push file bytes to a replica
              // that was just taken offline.
            } catch (Exception e) {
              System.out.println("TEST FAIL: replica " + r.id);
              e.printStackTrace(System.out);
              throw e;
            }
          } else {
            System.out.println("  skip down replica n=" + n);
          }
        }
      }

      if (master != null) {
        // Done pushing to all replicas so we now release
        // the files on master, so IW is free to delete if it
        // needs to:
        master.setInfos(infos);
      } else {
        if (masterDir != null) {
          System.out.println("TEST: close old master dir");
          masterDir.close();
        }
        masterLock.lock();
        try {
          int idx = random().nextInt(nodes.length);
          if (nodes[idx] == null) {
            // Directly start up Master:
            System.out.println("TEST: promote master on down node id=" + idx + " globalMaxVersion=" + globalMaxVersion + " globalMaxSegment=" + globalMaxSegment);
            nodes[idx] = master = new Master(globalMaxVersion, globalMaxSegment, dirs[idx], idx, nodes, versionDocCounts);
          } else {
            assert nodes[idx] instanceof Replica;
            System.out.println("TEST: promote node id=" + idx + " from replica to master; globalMaxVersion=" + globalMaxVersion + " globalMaxSegment=" + globalMaxSegment);
            master = new Master(globalMaxVersion, globalMaxSegment, (Replica) nodes[idx], nodes);
            nodes[idx] = master;
          }
        } finally {
          masterLock.unlock();
        }
      }
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

  static Map<String,Long> getFilesAndSizes(Directory dir, Collection<String> files) throws IOException {
    Map<String,Long> filesAndSizes = new HashMap<String,Long>();
    for(String file : files) {
      filesAndSizes.put(file, dir.fileLength(file));
    }
    return filesAndSizes;
  }

  static void copyFiles(Directory src, Replica dst, Map<String,Long> filesAndSizes, boolean lowPriority) throws IOException {
    long t0 = System.currentTimeMillis();
    long totBytes = 0;
    Set<String> toCopy = new HashSet<String>();
    for(Map.Entry<String,Long> ent : filesAndSizes.entrySet()) {
      if (dst.dir.fileExists(ent.getKey()) == false) {
        toCopy.add(ent.getKey());
        totBytes += ent.getValue();
      } else {
        System.out.println("  replica id=" + dst.id + ": skip copy file " + ent.getKey());
        assert dst.dir.fileLength(ent.getKey()) == ent.getValue() : "file size is wrong: current=" + dst.dir.fileLength(ent.getKey()) + " vs " + ent.getValue();
      }
    }

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
      System.out.println("  replica id=" + dst.id + ": copy file " + f);

      // Sync on each file to copy, so that MDW isn't closed
      // while a copy is happening, else MDW.close is angry:
      src.copy(dst.dir, f, f, ioContext);
      if (lowPriority) {
        try {
          Thread.sleep(bytes/100000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(ie);
        }
      }
    }
    long t1 = System.currentTimeMillis();
    System.out.println("  replica " + dst.id + ": " + (lowPriority ? "low-priority " : "") + "took " + (t1-t0) + " millis for " + totBytes + " bytes; " + toCopy.size() + " files (of " + filesAndSizes.size() + "); sizeInBytes=" + ((NRTCachingDirectory) dst.dir).sizeInBytes());
  }

  /** Like SearcherManager, except it refreshes via a
   *  provided (NRT) SegmentInfos. */
  private static class InfosSearcherManager extends ReferenceManager<IndexSearcher> {
    private SegmentInfos currentInfos;
    private final Directory dir;

    public InfosSearcherManager(Directory dir, int id) throws IOException {
      this.dir = dir;
      currentInfos = new SegmentInfos();
      String fileName = SegmentInfos.getLastCommitSegmentsFileName(dir);
      if (fileName != null) {
        // Load last commit:
        System.out.println("TEST id=" + id + ": replica load initial segments file " + fileName);
        currentInfos.read(dir, fileName);
        System.out.println("TEST id=" + id + ": replica loaded version=" + currentInfos.version + " docCount=" + docCount(currentInfos));
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
                  r.commit(false);
                }
                if (random().nextInt(100) == 17) {
                  // Shutdown this replica
                  nodes[i] = null;
                  r.shutdown();
                } else if (random().nextInt(100) == 17) {
                  // Crash the replica
                  nodes[i] = null;
                  r.crash();
                }
              } else if (master != null) {
                // Randomly commit master:
                if (random().nextInt(100) == 17) {
                  master.commit();
                }
              }
            } else if (random().nextInt(20) == 17) {
              // Restart this replica
              nodes[i] = new Replica(dirs[i], i, versionDocCounts, null);
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

  private static class Master {

    final Set<String> finishedMergedSegments = Collections.newSetFromMap(new ConcurrentHashMap<String,Boolean>());

    final Directory dir;
    final Object[] nodes;

    final RandomIndexWriter writer;
    final InfosSearcherManager mgr;
    final SearchThread searchThread;
    final IndexThread indexThread;
    final int id;
    //final Checksums checksums;

    SegmentInfos lastInfos;

    public Master(long globalMaxVersion, int globalMaxSegment, File path,
                  int id, Object[] nodes, Map<Long,Integer> versionDocCounts) throws IOException {
      final MockDirectoryWrapper dirOrig = newMockFSDirectory(path);

      // In some legitimate cases we will double-write:
      dirOrig.setPreventDoubleWrite(false);

      this.id = id;
      this.nodes = nodes;
      //checksums = new Checksums(dir);

      // nocommit put back
      dirOrig.setCheckIndexOnClose(false);

      dir = new NRTCachingDirectory(dirOrig, 1.0, 10.0);
      //((NRTCachingDirectory) master).VERBOSE = true;

      mgr = new InfosSearcherManager(dir, id);
      searchThread = new SearchThread("master", mgr, versionDocCounts);
      searchThread.start();

      SegmentInfos curInfos = mgr.getCurrentInfos().clone();
      curInfos.version = 1+globalMaxVersion;
      curInfos.counter = 1+globalMaxSegment;

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
      //checksums.save(dir);
      writer.w.commit();
    }

    /** Promotes an existing Replica to Master, re-using the
     *  open NRTCachingDir, the SearcherManager, the search
     *  thread, etc. */
    public Master(long globalMaxVersion, int globalMaxSegment, Replica replica, Object[] nodes) throws IOException {
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
      curInfos.version = 1+globalMaxVersion;
      curInfos.counter = 1+globalMaxSegment;

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
            //System.out.println("TEST: warm merged segment files " + info);
            Map<String,Long> filesAndSizes = getFilesAndSizes(dir, info.files());
            for(Object n : nodes) {
              if (n != null && n instanceof Replica) {
                try {
                  // nocommit do we need to check for merge aborted...?
                  ((Replica) n).warmMerge(info.info.name, dir, filesAndSizes);
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
      System.out.println("TEST id=" + id + ": close master waitForMerges=" + waitForMerges);

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
        System.out.println("TEST: waitForMerges done");
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

      System.out.println("TEST: close writer");

      // nocommit can we optionally NOT commit here?  it
      // should be decoupled from master migration?

      // Don't wait for merges now; we already did above:
      writer.close(false);
      System.out.println("TEST: done close writer");

      SegmentInfos infos = new SegmentInfos();
      infos.read(dir);

      System.out.println("TEST: return final infos=" + infos.toString(master.dir));

      System.out.println("TEST: close master dir");

      // nocommit caller must close
      // dir.close();

      return infos;
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
    final Directory dir;
    final InfosRefCounts deleter;

    private final InfosSearcherManager mgr;
    private volatile boolean stop;
    private SearchThread searchThread;

    private final Collection<String> lastCommitFiles;
    private final Collection<String> lastNRTFiles;

    // nocommit unused so far:
    private final IndexReaderWarmer mergedSegmentWarmer;

    public Replica(File path, int id, Map<Long,Integer> versionDocCounts, IndexReaderWarmer mergedSegmentWarmer) throws IOException {
      System.out.println("TEST id=" + id + ": replica startup");
      this.id = id;
      this.mergedSegmentWarmer = mergedSegmentWarmer;

      Directory fsDir = newMockFSDirectory(path);

      // In some legitimate cases we will double-write:
      ((MockDirectoryWrapper) fsDir).setPreventDoubleWrite(false);
      
      // nocommit put back
      ((BaseDirectoryWrapper) fsDir).setCheckIndexOnClose(false);

      dir = new NRTCachingDirectory(fsDir, 1.0, 10.0);

      // nocommit do this after sync?:
      mgr = new InfosSearcherManager(dir, id);
      lastCommitFiles = mgr.getCurrentInfos().files(dir, true);
      deleter = new InfosRefCounts(id, dir, lastCommitFiles);

      lastNRTFiles = new HashSet<String>(lastCommitFiles);
      deleter.incRef(lastNRTFiles);

      // Startup sync to pull latest index over:
      if (master != null) {
        masterLock.lock();
        SegmentInfos infos = master.getInfos();
        try {
          // Convert infos to byte[], to send "on the wire":
          RAMOutputStream out = new RAMOutputStream();
          infos.write(out);
          byte[] infosBytes = new byte[(int) out.getFilePointer()];
          out.writeTo(infosBytes, 0);

          Map<String,Long> filesAndSizes = getFilesAndSizes(master.dir, infos.files(master.dir, false));
          try {
            sync(master.dir, filesAndSizes, infosBytes, infos.version);
          } catch (Throwable t) {
            System.out.println("TEST id=" + id + ": FAIL replica");
            t.printStackTrace(System.out);
            throw new RuntimeException(t);
          }
        } finally {
          masterLock.unlock();
          master.releaseInfos(infos);
        }

        // Very important to do this: we may start up w/ a
        // later version against a master w/ an older
        // version, in which case we will have incorrect
        // "future" segment files:
        deleter.deleteUnknownFiles();
      }

      searchThread = new SearchThread(""+id, mgr, versionDocCounts);
      searchThread.start();
    }

    // nocommit move this to a thread so N replicas copy at
    // once:

    public synchronized void sync(Directory master, Map<String,Long> filesAndSizes, byte[] infosBytes,
                                  long infosVersion) throws IOException {

      if (stop) {
        throw new AlreadyClosedException("replica closed");
      }

      SegmentInfos currentInfos = mgr.getCurrentInfos();
      System.out.println("TEST id=" + id + ": replica sync version=" + infosVersion + " vs current version=" + currentInfos.getVersion());

      /*
      if (currentInfos != null && currentInfos.getVersion() >= infosVersion) {
        System.out.println("  replica id=" + id + ": skip sync current version=" + currentInfos.getVersion() + " vs new version=" + infosVersion);
        return;
      }
      */

      // Copy files over to replica:
      copyFiles(master, this, filesAndSizes, false);

      // Turn byte[] back to SegmentInfos:
      SegmentInfos infos = new SegmentInfos();
      infos.read(dir, new ByteArrayDataInput(infosBytes));
      System.out.println("TEST id=" + id + ": replica sync version=" + infos.version + " segments=" + infos.toString(dir));

      // Cutover to new searcher
      mgr.refresh(infos);

      // Delete now un-referenced files:
      Collection<String> newFiles = infos.files(dir, false);
      deleter.incRef(newFiles);
      deleter.decRef(lastNRTFiles);
      lastNRTFiles.clear();
      lastNRTFiles.addAll(newFiles);
    }

    public synchronized void warmMerge(String segmentName, Directory master, Map<String,Long> filesAndSizes) throws IOException {
      if (stop) {
        throw new AlreadyClosedException("replica closed");
      }
      System.out.println("TEST id=" + id + ": replica warm merge " + segmentName);
      copyFiles(master, this, filesAndSizes, true);

      // nocommit we could also pre-warm a SegmentReader
      // here, and add it onto subReader list for next reopen ...?
    }

    /** Gracefully close & shutdown this replica. */
    public synchronized void shutdown() throws IOException, InterruptedException {
      stop = true;
      // Sometimes shutdown w/o commiting
      System.out.println("TEST id=" + id + ": replica shutdown");
      if (random().nextBoolean()) {
        System.out.println("TEST id=" + id + ": replica commit before shutdown");
        commit(true);
      }

      searchThread.finish();
      mgr.close();

      // Delete now un-referenced files:
      deleter.deletePending();
      dir.close();
    }

    /** Crashes the underlying directory, corrupting any
     *  un-sync'd files. */
    public synchronized void crash() throws IOException, InterruptedException {
      stop = true;
      System.out.println("TEST id=" + id + ": replica crash");
      searchThread.finish();
      mgr.close();
      ((MockDirectoryWrapper) ((NRTCachingDirectory) dir).getDelegate()).crash();
      ((NRTCachingDirectory) dir).getDelegate().close();
    }

    /** Commit latest SegmentInfos (fsync'ing all referenced
     *  files). */
    public synchronized void commit(boolean deleteAll) throws IOException {
      SegmentInfos infos = mgr.getCurrentInfos();
      if (infos != null) {
        System.out.println("TEST id=" + id + ": replica commit deleteAll=" + deleteAll + "; infos.version=" + infos.getVersion() + " files=" + infos.files(dir, false));
        dir.sync(infos.files(dir, false));
        infos.commit(dir);
        System.out.println("TEST id=" + id + ": replica commit segments file: " + infos.getSegmentsFileName());

        Collection<String> newFiles = infos.files(dir, true);
        deleter.incRef(newFiles);
        deleter.decRef(lastCommitFiles);
        lastCommitFiles.clear();
        lastCommitFiles.addAll(newFiles);

        if (deleteAll) {
          // nocommit this is messy: we may delete a merge's files
          // that just copied over as we closed the writer:
          deleter.deleteUnknownFiles();
        }
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
              assertEquals("searcher version=" + version + " replica id=" + id + " searcher=" + s, expectedCount.intValue(), totalHits);
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

    public InfosRefCounts(int id, Directory dir, Collection<String> commitFileNames) throws IOException {
      this.dir = dir;
      this.id = id;

      incRef(commitFileNames);

      // Must delete unused files on startup: we could have
      // crashed, and copied-but-not-sync'd files may now be
      // corrupt.  We can only trust the files referenced by
      // the commit point we just loaded:
      deleteUnknownFiles();
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
        System.out.println("  replica id=" + id + ": delete " + fileName);
        dir.deleteFile(fileName);
      } catch (IOException ioe) {
        // nocommit why do we keep trying to delete the file
        // "segments" ...
        System.out.println("  replica id=" + id + ": delete " + fileName + " failed; will retry later");
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
        if (refCounts.containsKey(fileName) == false) {
          delete(fileName);
        }
      }
    }
  }

  // nocommit unused currently:
  static class Checksums {

    private static final String FILE_NAME_PREFIX = "checksums";
    private static final String CHECKSUM_CODEC = "checksum";
    private static final int CHECKSUM_VERSION_START = 0;
    private static final int CHECKSUM_VERSION_CURRENT = CHECKSUM_VERSION_START;

    private final Map<String,Long> checksums = new HashMap<String,Long>();

    private long nextWriteGen;

    // nocommit need to test crashing after writing checksum
    // & before committing

    public Checksums(Directory dir) throws IOException {
      long maxGen = -1;
      for (String fileName : dir.listAll()) {
        if (fileName.startsWith(FILE_NAME_PREFIX)) {
          long gen = Long.parseLong(fileName.substring(1+FILE_NAME_PREFIX.length()),
                                    Character.MAX_RADIX);
          if (gen > maxGen) {
            maxGen = gen;
          }
        }
      }

      while (maxGen > -1) {
        IndexInput in = dir.openInput(genToFileName(maxGen), IOContext.DEFAULT);
        try {
          int version = CodecUtil.checkHeader(in, CHECKSUM_CODEC, CHECKSUM_VERSION_START, CHECKSUM_VERSION_START);
          if (version != CHECKSUM_VERSION_START) {
            throw new CorruptIndexException("wrong checksum version");
          }
          int count = in.readVInt();
          for(int i=0;i<count;i++) {
            String name = in.readString();
            long checksum = in.readLong();
            add(name, checksum);
          }
          nextWriteGen = maxGen+1;
          break;
        } catch (IOException ioe) {
          // This file was truncated, probably due to
          // crashing w/o syncing:
          maxGen--;
        } finally {
          in.close();
        }
      }
    }

    private static String genToFileName(long gen) {
      return FILE_NAME_PREFIX + "_" + Long.toString(gen, Character.MAX_RADIX);
    }
    
    public synchronized void add(String name, long checksum) {
      checksums.put(name, checksum);
    }

    public synchronized long get(String name) {
      return checksums.get(name);
    }

    public synchronized void save(Directory dir) throws IOException {
      String fileName = genToFileName(nextWriteGen++);
      IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT);
      try {
        CodecUtil.writeHeader(out, CHECKSUM_CODEC, CHECKSUM_VERSION_CURRENT);
        out.writeVInt(checksums.size());
        for(Map.Entry<String,Long> ent : checksums.entrySet()) {
          out.writeString(ent.getKey());
          out.writeLong(ent.getValue());
        }
      } finally {
        out.close();
      }

      dir.sync(Collections.singletonList(fileName));
    }
  }
}
