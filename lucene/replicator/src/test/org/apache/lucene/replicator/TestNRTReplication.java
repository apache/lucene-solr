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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
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
import org.apache.lucene.store.RateLimitedIndexOutput;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.TreeLogger;
import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;

// nocommit add SLM

// nocommit also allow downgrade of Master to Replica,
// instead of Master.close then Replica init

// nocommit make sure we are not over-IncRef'ing infos on master

// nocommit test replicas that are slow to copy

// nocommit add fang: master crashes

// nocommit provoke more "going backwards", e.g. randomly
// sometimes shutdown whole cluster and pick random node to
// be the new master

// nocommit should we have support for "flush as frequently
// as you can"?  or at least, "do not flush so frequently
// that replicas can't finish copying before next flush"?

// nocommit what about network partitioning

// nocommit make MDW throw exceptions sometimes

// nocommit test warming merge w/ O_DIRECT via NativeUnixDir

// nocommit test rare bit errors during copy

// nocommit test slow replica

// nocommit test replica that "falls out" because it's too
// slow and then tries to join back again w/o having
// "properly" restarted

// nocommit rewrite the test so each node has its own
// threads, to be closer to the concurrency we'd "really"
// see across N machines

// nocommit also allow replicas pulling files from replicas;
// they need not always come from master

@SuppressCodecs({ "SimpleText", "Memory", "Direct" })
public class TestNRTReplication extends LuceneTestCase {

  static volatile Master master;
  static final AtomicInteger masterCount = new AtomicInteger();
  static final Lock masterLock = new ReentrantLock();
  static Object[] nodes;

  @AfterClass
  public static void afterClass() {
    System.out.println("TEST: now afterClass");
    master = null;
    nodes = null;
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

    Thread.currentThread().setName("main");
    TreeLogger.setLogger(new TreeLogger("main"));

    // Maps all segmentInfos.getVersion() we've seen, to the
    // expected doc count.  On each replica when we do a
    // *:* search we verify the totalHits is correct:
    final Map<Long,Integer> versionDocCounts = new ConcurrentHashMap<Long,Integer>();

    int numDirs = 1+_TestUtil.nextInt(random(), 2, 6);
    // nocommit
    //int numDirs = 2;

    final File[] dirs = new File[numDirs];

    // One Master (initially node 0) and N-1 Replica:
    nodes = new Object[numDirs];
    System.out.println("TEST: " + nodes.length + " nodes");
    for(int i=0;i<numDirs;i++) {
      dirs[i] = _TestUtil.getTempDir("NRTReplication." + i + "_");
      if (i > 0) {
        // Some replicas don't start on init:
        if (random().nextInt(10) < 7) {
          nodes[i] = new Replica(dirs[i], i, versionDocCounts, null);
        } else {
          System.out.println("TEST: skip replica " + i + " startup");
        }
      }
    }

    nodes[0] = master = new Master(dirs[0], 0, versionDocCounts);

    // nocommit test graceful full shutdown / restart

    // nocommit test all nodes hard shutdown

    long endTime = System.currentTimeMillis() + 60000;
    //long endTime = System.currentTimeMillis() + 10000;

    while (System.currentTimeMillis() < endTime) {
      Thread.sleep(_TestUtil.nextInt(random(), 2, 50));

      assert master != null;

      TreeLogger.log("\nTEST: now replicate master id=" + master.id);
      TreeLogger.start("replicate");

      SegmentInfos infos;
      boolean closeMaster = random().nextInt(100) == 57;
      if (closeMaster) {
        TreeLogger.log("top: id=" + master.id + " now move master");
        // Commits & closes current master and pull the
        // infos of the final commit:
        master.close(random().nextBoolean());
        TreeLogger.log("top: done shutdown master");
      } else {

        // Have writer do a full flush, and return the
        // resulting segments, protected from deletion
        // (incRef'd) just while we copy the files out to
        // the replica (s).  This is just like pulling an
        // NRT reader, except we don't actually open the
        // readers on the newly flushed segments:
        TreeLogger.log("flush current master");
        master.flush();
        TreeLogger.log("done flush current master");
      }

      CopyState copyState = master.getCopyState();

      // nocommit also allow downgrade Master -> Replica,
      // NOT a full close

      int count = docCount(copyState.infos);
      TreeLogger.log("record version=" + copyState.version + " count=" + count + " segs=" + copyState.infos.toString(copyState.dir));
      Integer oldCount = versionDocCounts.put(copyState.version, count);

      // Refresh the local searcher on master:
      if (closeMaster == false) {
        master.mgr.setCurrentInfos(copyState.infos);
        master.mgr.maybeRefresh();
      }

      // nocommit break this into separate tests, so we can
      // test the "clean" case where versions are "correct":

      // nocommit cannot do this: versions can go backwards
      //if (oldCount != null) {
      //assertEquals("version=" + infos.version + " oldCount=" + oldCount + " newCount=" + count, oldCount.intValue(), count);
      //}

      // nocommit can we have commit commit the "older" SIS?
      // they will commit quickly since the OS will have
      // already moved those bytes to disk...

      String extra = " master.sizeInBytes=" + ((NRTCachingDirectory) master.dir.getDelegate()).sizeInBytes();

      TreeLogger.log("replicate docCount=" + count + " version=" + copyState.version + extra + " segments=" + copyState.infos.toString(copyState.dir));

      // nocommit test master crash (promoting replica to master)

      // nocommit simulate super-slow replica: it should not
      // hold up the copying of other replicas, nor new
      // flushing; the copy of a given SIS to a given
      // replica should be fully concurrent/bg

      // Notify all running replicas that they should now
      // pull the new flush over:
      int upto = 0;
      for(Object n : nodes) {
        if (n != null && n instanceof Replica) {
          Replica r = (Replica) n;
          // nocommit can we "broadcast" the new files
          // instead of each replica pulling its own copy
          // ...
          TreeLogger.log("id=" + upto + ": signal new flush");
          r.newFlush();
        } else if (n == null) {
          TreeLogger.log("id=" + upto + " skip down replica");
        }
        upto++;
      }

      master.releaseCopyState(copyState);

      if (closeMaster) {
        if (random().nextBoolean()) {
          TreeLogger.log("top: id=" + master.id + " now waitIdle");
          master.waitIdle();
          TreeLogger.log("top: id=" + master.id + " done waitIdle");
        } else {
          TreeLogger.log("top: id=" + master.id + " skip waitIdle");
          Thread.sleep(random().nextInt(5));
        }

        TreeLogger.log("top: id=" + master.id + " close old master dir dir.listAll()=" + Arrays.toString(master.dir.listAll()));
        master.dir.close();

        masterLock.lock();
        try {
          masterCount.incrementAndGet();
        } finally {
          masterLock.unlock();
        }

        nodes[master.id] = null;

        // nocommit make sure we test race here, where
        // replica is coming up just as we are electing a
        // new master

        // Must pick newest replica to promote, else we
        // can't overwrite open files when trying to copy
        // to the newer replicas:
        int bestIDX = -1;
        long highestVersion = -1;
        for (int idx=0;idx<nodes.length;idx++) {
          if (nodes[idx] instanceof Replica) {
            Replica r = (Replica) nodes[idx];
            long version = r.mgr.getCurrentInfos().version;
            TreeLogger.log("top: id=" + r.id + " check version=" + version);
            if (version > highestVersion) {
              bestIDX = idx;
              highestVersion = version;
              TreeLogger.log("top: id=" + r.id + " check version=" + version + " max so far");
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
          nodes[idx] = master = new Master(dirs[idx], idx, versionDocCounts);
        } else {
          // Promote a running replica to Master:
          assert nodes[idx] instanceof Replica;
          TreeLogger.log("top: id=" + idx + " promote replica to master");
          master = new Master((Replica) nodes[idx]);
          nodes[idx] = master;
        }
      } else {
        if (random().nextInt(100) == 17) {
          TreeLogger.log("top: id=" + master.id + " commit master");
          master.commit();
        }
      }

      // Maybe restart a down replica, or commit / shutdown
      // / crash one:
      for(int i=0;i<nodes.length;i++) {
        if (nodes[i] == null && random().nextInt(20) == 17) {
          // Restart this replica
          try {
            nodes[i] = new Replica(dirs[i], i, versionDocCounts, null);
          } catch (Throwable t) {
            TreeLogger.log("top: id=" + i + " FAIL startup", t);
            throw t;
          }
        } else {
          if (nodes[i] instanceof Replica) {
            Replica r = (Replica) nodes[i];

            if (random().nextInt(100) == 17) {
              TreeLogger.log("top: id=" + i + " commit replica");
              r.commit(false);
            }

            if (random().nextInt(100) == 17) {
              // Now shutdown this replica
              TreeLogger.log("top: id=" + i + " shutdown replica");
              r.shutdown();
              nodes[i] = null;
              break;
            } else if (random().nextInt(100) == 17) {
              // Now crash the replica
              TreeLogger.log("top: id=" + i + " crash replica");
              r.crash();
              nodes[i] = null;
              break;
            }
          }
        }
      }

      TreeLogger.end("replicate");
    }

    System.out.println("TEST: close replicas");
    for(Object n : nodes) { 
      if (n != null && n instanceof Replica) {
        ((Replica) n).shutdown();
      }
    }

    if (master != null) {
      System.out.println("TEST: close master");
      master.close(random().nextBoolean());
      master.dir.close();
    }
  }

  static class FileMetaData {
    public final long length;
    public final long checksum;

    public FileMetaData(long length, long checksum) {
      this.length = length;
      this.checksum = checksum;
    }
  }

  static Map<String,FileMetaData> getFilesMetaData(Master master, Collection<String> files) throws IOException {
    Map<String,FileMetaData> filesMetaData = new HashMap<String,FileMetaData>();
    for(String file : files) {
      filesMetaData.put(file, new FileMetaData(master.dir.fileLength(file), master.dir.getChecksum(file)));
    }

    return filesMetaData;
  }

  static Map<String,FileMetaData> copyFiles(SlowChecksumDirectory src, Replica dst, Map<String,FileMetaData> files, boolean lowPriority, long totBytes) throws IOException {
    long t0 = System.currentTimeMillis();

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

    CopyResult copyResult = dst.fileCopier.add(src, files, lowPriority, ioContext);

    try {
      copyResult.done.await();
    } catch (InterruptedException ie) {
      // nocommit
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    }

    if (copyResult.failed.get()) {
      TreeLogger.log("replica " + dst.id + ": " + (lowPriority ? "low-priority " : "") + " failed to copy some files");
      return null;
    }
    
    long t1 = System.currentTimeMillis();
    TreeLogger.log("replica " + dst.id + ": " + (lowPriority ? "low-priority " : "") + "took " + (t1-t0) + " millis for " + totBytes + " bytes; " + files.size() + " files; nrtDir.sizeInBytes=" + ((NRTCachingDirectory) dst.dir.getDelegate()).sizeInBytes());

    return files;
  }

  /** Like SearcherManager, except it refreshes via an
   *  externally provided (NRT) SegmentInfos. */
  private static class InfosSearcherManager extends ReferenceManager<IndexSearcher> {
    private volatile SegmentInfos currentInfos;
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

    public void setCurrentInfos(SegmentInfos infos) {
      if (currentInfos != null) {
        // So that if we commit, we will go to the next
        // (unwritten so far) generation:
        infos.updateGeneration(currentInfos);
        TreeLogger.log("mgr.setCurrentInfos: carry over infos gen=" + infos.getSegmentsFileName());
      }
      currentInfos = infos;
    }

    // nocommit who manages the "ref" for currentInfos?

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

  private static class CopyState {
    public final SlowChecksumDirectory dir;
    public final Map<String,FileMetaData> files;
    public final long version;
    public final byte[] infosBytes;
    public final SegmentInfos infos;

    public CopyState(SlowChecksumDirectory dir, Map<String,FileMetaData> files, long version, byte[] infosBytes, SegmentInfos infos) {
      this.dir = dir;
      this.files = files;
      this.version = version;
      this.infosBytes = infosBytes;
      this.infos = infos;
    }
  }

  // nocommit should extend replica and just add writer
  private static class Master {

    final Set<String> finishedMergedSegments = Collections.newSetFromMap(new ConcurrentHashMap<String,Boolean>());

    final SlowChecksumDirectory dir;

    final RandomIndexWriter writer;
    final InfosSearcherManager mgr;
    final SearchThread searchThread;
    final IndexThread indexThread;
    final int id;
    private volatile boolean isClosed;
    private AtomicInteger infosRefCount = new AtomicInteger();

    SegmentInfos lastInfos;

    /** Start up a master from scratch. */
    public Master(File path,
                  int id, Map<Long,Integer> versionDocCounts) throws IOException {
      final MockDirectoryWrapper dirOrig = newMockFSDirectory(path);

      // In some legitimate cases we will double-write:
      dirOrig.setPreventDoubleWrite(false);

      this.id = id;

      // nocommit put back
      dirOrig.setCheckIndexOnClose(false);

      // Master may legitimately close while replicas are
      // still copying from it:
      dirOrig.setAllowCloseWithOpenFiles(true);

      dir = new SlowChecksumDirectory(id, new NRTCachingDirectory(dirOrig, 1.0, 10.0));
      //((NRTCachingDirectory) master).VERBOSE = true;
      SegmentInfos infos = new SegmentInfos();
      try {
        infos.read(dir);
      } catch (IndexNotFoundException infe) {
      }

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
      setCopyState();

      indexThread = new IndexThread(this);
      indexThread.start();
    }

    public void commit() throws IOException {
      writer.w.prepareCommit();
      // It's harmless if we crash here, because the
      // global state has already been updated
      writer.w.commit();
    }

    /** Promotes an existing Replica to Master, re-using the
     *  open NRTCachingDir, the SearcherManager, the search
     *  thread, etc. */
    public Master(Replica replica) throws IOException {
      this.id = replica.id;
      this.dir = replica.dir;
      this.mgr = replica.mgr;
      this.searchThread = replica.searchThread;

      // Master may legitimately close while replicas are
      // still copying from it:
      ((MockDirectoryWrapper) ((NRTCachingDirectory) dir.getDelegate()).getDelegate()).setAllowCloseWithOpenFiles(true);

      // Do not copy from ourself:
      try {
        replica.copyThread.finish();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ie);
      }
      replica.fileCopier.close();

      // nocommit must somehow "stop" this replica?  e.g. we
      // don't want it doing any more deleting?

      IndexWriterConfig iwc = getIWC();
      iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);

      // nocommit also test periodically committing, and
      // preserving multiple commit points; verify these
      // "survive" over to the replica
      SegmentInfos infos = replica.mgr.getCurrentInfos().clone();
      TreeLogger.log("top: id=" + replica.id + " sis version=" + infos.version);

      writer = new RandomIndexWriter(random(), dir, replica.mgr.getCurrentInfos().clone(), iwc);
      _TestUtil.reduceOpenFiles(writer.w);
      System.out.println("after reduce: " + writer.w.getConfig());

      lastInfos = mgr.getCurrentInfos();

      writer.w.incRefDeleter(lastInfos);

      setCopyState();

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
            Map<String,FileMetaData> toCopy = getFilesMetaData(Master.this, info.files());
            long totBytes = 0;
            for(FileMetaData metaData : toCopy.values()) {
              totBytes += metaData.length;
            }
            TreeLogger.log("warm files=" + toCopy.keySet() + " totBytes=" + totBytes);
            TreeLogger.start("warmMerge");

            IOContext context = new IOContext(new MergeInfo(0, totBytes, false, 0));

            List<CopyResult> copyResults = new ArrayList<>();
            for(Object n : nodes) {
              if (n != null && n instanceof Replica) {
                Replica r = (Replica) n;
                
                try {

                  // nocommit we could also have replica pre-warm a SegmentReader
                  // here, and add it onto subReader list
                  // for next reopen ...?

                  // Must call filter, in case we are
                  // overwriting files and must invalidate
                  // the last commit:
                  int sizeBefore = toCopy.size();
                  Map<String,FileMetaData> toCopy2 = r.filterFilesToCopy(toCopy);

                  // Since this is a newly merged segment,
                  // all files should be new and need
                  // copying:
                  assert toCopy.size() == sizeBefore: "before=" + toCopy.keySet() + " after=" + toCopy2.keySet();

                  TreeLogger.log("copy to replica " + r.id);
                  copyResults.add(r.copyMergedFiles(Master.this.dir, toCopy2, context));
                } catch (AlreadyClosedException ace) {
                  // Ignore this: it means the replica shut down
                  // while we were trying to copy files.  This
                  // "approximates" an exception the master would
                  // see trying to push file bytes to a replica
                  // that was just taken offline.
                }
              }
            }

            TreeLogger.log("now wait for " + copyResults.size() + " replicas to finish copying");
            for(CopyResult result : copyResults) {
              try {
                result.done.await();
              } catch (InterruptedException ie) {
                // nocommit
              }

              // nocommit if there's an error ... what to
              // do?

              // nocommit we should check merge.abort
              // somehow in here, so if the master is in
              // a hurry to shutdown, we respect that
            }
            TreeLogger.log("done warm merge for " + copyResults.size() + " replicas");
            TreeLogger.end("warmMerge");
          }
        });

      return iwc;
    }

    /** Gracefully shuts down the master */
    public void close(boolean waitForMerges) throws IOException {
      TreeLogger.log("id=" + id + " close master waitForMerges=" + waitForMerges);

      try {
        searchThread.finish();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ie);
      }

      if (waitForMerges) {
        // Do it here, instead of on close, so we can
        // continue indexing while waiting for merges:
        writer.w.waitForMerges();
        TreeLogger.log("waitForMerges done");
      }

      mgr.close();

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

      synchronized(this) {
        isClosed = true;
        if (lastInfos != null) {
          writer.w.decRefDeleter(lastInfos);
          lastInfos = null;
          copyState = null;
        }

        // nocommit what about exc from writer.close...
        // Don't wait for merges now; we already did above:
        writer.close(false);
        TreeLogger.log("done close writer");

        SegmentInfos infos = new SegmentInfos();
        infos.read(dir);

        TreeLogger.log("final infos=" + infos.toString(master.dir));

        // nocommit caller must close
        // dir.close();

        lastInfos = infos;
        setCopyState();
      }
    }

    public synchronized boolean isClosed() {
      return isClosed;
    }

    private CopyState copyState;

    private synchronized void setCopyState() throws IOException {
      RAMOutputStream out = new RAMOutputStream();
      lastInfos.write(out);
      byte[] infosBytes = new byte[(int) out.getFilePointer()];
      out.writeTo(infosBytes, 0);
      copyState = new CopyState(dir, 
                                getFilesMetaData(this, lastInfos.files(dir, false)),
                                lastInfos.version, infosBytes, lastInfos);
    }

    public synchronized CopyState getCopyState() throws IOException {
      //if (isClosed && lastInfos == null) {
      //return null;
      //}
      TreeLogger.log("master.getCopyState version=" + lastInfos.version + " files=" + lastInfos.files(dir, false) + " writer=" + writer);
      if (isClosed == false) {
        writer.w.incRefDeleter(copyState.infos);
      }
      int count = infosRefCount.incrementAndGet();
      assert count >= 1;
      return copyState;
    }

    /** Flushes, returns a ref with the resulting infos. */
    public void flush() throws IOException {
      SegmentInfos newInfos = master.writer.w.flushAndIncRef();

      synchronized(this) {
        writer.w.decRefDeleter(lastInfos);
        // Steals the reference returned by IW:
        lastInfos = newInfos;
        setCopyState();
      }
    }

    public synchronized void releaseCopyState(CopyState copyState) throws IOException {
      TreeLogger.log("master.releaseCopyState version=" + copyState.version + " files=" + copyState.files.keySet());
      // Must check because by the time the replica releases
      // it's possible it's a different master:
      if (copyState.dir == dir) {
        if (isClosed == false) {
          writer.w.decRefDeleter(copyState.infos);
        }
        int count = infosRefCount.decrementAndGet();
        assert count >= 0;
        TreeLogger.log("  infosRefCount=" + infosRefCount);
        if (count == 0) {
          notify();
        }
      } else {
        TreeLogger.log("  skip: wrong master");
      }
    }

    /** Waits until all outstanding infos refs are dropped. */
    public synchronized void waitIdle() throws InterruptedException {
      while (true) {
        if (infosRefCount.get() == 0) {
          return;
        }
        wait();
      }
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
  }

  private static class Replica {
    final int id;
    final SlowChecksumDirectory dir;
    final InfosRefCounts deleter;

    private final InfosSearcherManager mgr;
    private volatile boolean stop;
    final SearchThread searchThread;
    final SimpleFileCopier fileCopier;
    CopyThread copyThread;

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

      fileCopier = new SimpleFileCopier(dir, id);
      fileCopier.start();

      lastCommitFiles = new HashSet<String>();
      lastNRTFiles = new HashSet<String>();

      deleter = new InfosRefCounts(id, dir);

      String segmentsFileName = SegmentInfos.getLastCommitSegmentsFileName(dir);
      SegmentInfos lastCommit = null;
      if (segmentsFileName != null) {
        lastCommit = new SegmentInfos();
        TreeLogger.log("id=" + id + " " + segmentsFileName + " now load");
        lastCommit.read(dir, segmentsFileName);
        lastCommitFiles.addAll(lastCommit.files(dir, true));
        TreeLogger.log("id=" + id + " incRef lastCommitFiles");
        deleter.incRef(lastCommitFiles);
        TreeLogger.log("id=" + id + " loaded version=" + lastCommit.version + " lastCommitFiles = " + lastCommitFiles);
      }

      // Must sync latest index from master before starting
      // up mgr, so that we don't hold open any files that
      // need to be overwritten when the master is against
      // an older index than our copy, and so we rollback
      // our version if we had been at a higher version but
      // were down when master moved:
      SegmentInfos infos = null;

      // Startup sync to pull latest index over:
      Map<String,FileMetaData> copiedFiles = null;

      if (master != null) {

        // nocommit this logic isn't right; else, on a full
        // restart how will a master be found:

        // Repeat until we find a working master; this is to
        // handle the case when a replica starts up but no
        // new master has yet been selected when moving
        // master:
        while (true) {
          Master curMaster = master;
          CopyState copyState = curMaster.getCopyState();
          try {
            // nocommit factor this out & share w/ CopyThread:

            deleter.incRef(copyState.files.keySet());

            Map<String,FileMetaData> toCopy = filterFilesToCopy(copyState.files);
            long totBytes = 0;
            for(FileMetaData metaData : toCopy.values()) {
              totBytes += metaData.length;
            }

            // Copy files over to replica:
            copiedFiles = copyFiles(copyState.dir, this, toCopy, false, totBytes);
            if (copiedFiles == null) {
              TreeLogger.log("id=" + id + " startup copyFiles failed");
              deleter.decRef(copyState.files.keySet());
              try {
                Thread.sleep(5);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
              }
              continue;
            }
      
            // Turn byte[] back to SegmentInfos:
            infos = new SegmentInfos();
            infos.read(dir, new ByteArrayDataInput(copyState.infosBytes));
            lastNRTFiles.addAll(copyState.files.keySet());
          } finally {
            curMaster.releaseCopyState(copyState);
          }
          break;
        }
      } else {
        TreeLogger.log("id=" + id + " no master on startup; fallback to last commit");
      }

      if (lastCommit != null) {
        infos.updateGeneration(lastCommit);
      }

      mgr = new InfosSearcherManager(dir, id, infos);

      deleter.deleteUnknownFiles();

      searchThread = new SearchThread(""+id, mgr, versionDocCounts);
      searchThread.start();

      copyThread = new CopyThread(this);
      copyThread.start();

      TreeLogger.log("done startup");
      TreeLogger.end("startup");
    }

    /** From the incoming files, determines which ones need
     *  copying on this replica, because they weren't yet
     *  copied, or they were previously copied but the
     *  checksum or file length is different. */
    public Map<String,FileMetaData> filterFilesToCopy(Map<String,FileMetaData> files) throws IOException {
      Map<String,FileMetaData> toCopy = new HashMap<>();

      for(Map.Entry<String,FileMetaData> ent : files.entrySet()) {
        String fileName = ent.getKey();
        FileMetaData metaData = ent.getValue();
        long srcChecksum = ent.getValue().checksum;

        Long checksum = dir.getChecksum(fileName);

        long curLength;
        try {
          curLength = dir.fileLength(fileName);
        } catch (FileNotFoundException | NoSuchFileException e) {
          curLength = -1;
        }
        if (curLength != metaData.length) {
          TreeLogger.log("id=" + id + " " + fileName + " will copy [wrong file length old=" + curLength + " new=" + metaData.length + "]");
          toCopy.put(fileName, metaData);
        } else if (checksum == null) {
          TreeLogger.log("id=" + id + " " + fileName + " will copy [no checksum] length=" + metaData.length + " srcChecksum=" + srcChecksum);
          toCopy.put(fileName, metaData);
        } else if (checksum.longValue() != srcChecksum) {
          TreeLogger.log("id=" + id + " " + fileName + " will copy [wrong checksum: cur=" + checksum.longValue() + " new=" + srcChecksum + "] length=" + metaData.length);
          toCopy.put(fileName, metaData);
        } else {
          TreeLogger.log("id=" + id + " " + fileName + " skip copy checksum=" + srcChecksum + " file.length=" + metaData.length);
        }
      }

      // nocommit what about multiple commit points?

      // Invalidate the current commit if we will overwrite
      // any files from it:

      // nocommit do we need sync'd here?
      for(String fileName : toCopy.keySet()) {
        if (lastCommitFiles.contains(fileName)) {
          TreeLogger.log("id=" + id + " " + fileName + " is being copied but is referenced by last commit; now drop last commit; lastCommitFiles=" + lastCommitFiles);
          deleter.decRef(lastCommitFiles);
          dir.deleteFile("segments.gen");
          lastCommitFiles.clear();
          break;
        }
      }

      return toCopy;
    }

    public void newFlush() {
      copyThread.lock.lock();
      try {
        copyThread.cond.signal();
      } finally {
        copyThread.lock.unlock();
      }
    }

    public CopyResult copyMergedFiles(SlowChecksumDirectory src, Map<String,FileMetaData> files, IOContext context) throws IOException {
      if (stop) {
        throw new AlreadyClosedException("replica closed");
      }
      return fileCopier.add(src, files, true, context);
    }

    void sync(int curMasterMoveCount, SlowChecksumDirectory master, Map<String,FileMetaData> filesMetaData, byte[] infosBytes,
              long infosVersion) throws IOException {

      SegmentInfos currentInfos = mgr.getCurrentInfos();
      TreeLogger.log("id=" + id + " sync version=" + infosVersion + " vs current version=" + currentInfos.getVersion());
      TreeLogger.start("sync");

      // nocommit make overall test "modal", ie up front
      // decide whether any docs are allowed to be lost
      // (version goes backwards on replicas) or not
 
      /*
        if (currentInfos != null && currentInfos.getVersion() >= infosVersion) {
        System.out.println("  replica id=" + id + ": skip sync current version=" + currentInfos.getVersion() + " vs new version=" + infosVersion);
        return;
        }
      */

      // Must incRef before filter in case filter decRefs
      // the last commit:
      deleter.incRef(filesMetaData.keySet());

      Map<String,FileMetaData> toCopy = filterFilesToCopy(filesMetaData);
      long totBytes = 0;
      for(FileMetaData metaData : toCopy.values()) {
        totBytes += metaData.length;
      }

      // Copy files over to replica:
      Map<String,FileMetaData> copiedFiles = copyFiles(master, this, toCopy, false, totBytes);

      if (copiedFiles == null) {
        // At least one file failed to copy; skip cutover
        TreeLogger.log("top: id=" + id + " replica sync failed; abort");
        deleter.decRef(filesMetaData.keySet());
        TreeLogger.end("sync");
        return;
      }

      TreeLogger.log("top: id=" + id + " replica sync done file copy");

      // OK all files copied successfully, now rebuild the
      // infos and cutover searcher mgr

      // Turn byte[] back to SegmentInfos:
      SegmentInfos infos = new SegmentInfos();
      infos.read(dir, new ByteArrayDataInput(infosBytes));
      TreeLogger.log("id=" + id + " replica sync version=" + infos.version + " segments=" + infos.toString(dir));

      masterLock.lock();
      try {
        if (curMasterMoveCount != masterCount.get()) {
          // At least one file failed to copy; skip cutover
          TreeLogger.log("top: id=" + id + " master moved during sync; abort");
          deleter.decRef(filesMetaData.keySet());
          TreeLogger.end("sync");
          return;
        }

        synchronized (this) {
          mgr.setCurrentInfos(infos);
          deleter.decRef(lastNRTFiles);
          lastNRTFiles.clear();
          lastNRTFiles.addAll(filesMetaData.keySet());
        }
      } finally {
        masterLock.unlock();
      }

      // Cutover to new searcher
      mgr.maybeRefresh();

      TreeLogger.log("top: id=" + id + " done mgr refresh");
      
      TreeLogger.end("sync");
    }

    /** Gracefully close & shutdown this replica. */
    public void shutdown() throws IOException, InterruptedException {
      if (stop) {
        return;
      }
      stop = true;

      copyThread.finish();
      fileCopier.close();

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
    public void crash() throws IOException, InterruptedException {
      if (stop) {
        return;
      }
      stop = true;
      TreeLogger.log("id=" + id + " replica crash; dir.listAll()=" + Arrays.toString(dir.listAll()));
      TreeLogger.log("id=" + id + " replica crash; fsdir.listAll()=" + Arrays.toString(((NRTCachingDirectory) dir.getDelegate()).getDelegate().listAll()));
      copyThread.finish();
      fileCopier.close();
      searchThread.finish();
      mgr.close();
      ((MockDirectoryWrapper) ((NRTCachingDirectory) dir.getDelegate()).getDelegate()).crash();
      ((NRTCachingDirectory) dir.getDelegate()).getDelegate().close();
    }

    /** Commit latest SegmentInfos (fsync'ing all referenced
     *  files). */
    public void commit(boolean deleteAll) throws IOException {
      SegmentInfos infos;
      Collection<String> newFiles;

      synchronized (this) {
        infos = mgr.getCurrentInfos();
        if (infos != null) {
          newFiles = infos.files(dir, false);
          deleter.incRef(newFiles);
        } else {
          // nocommit is infos ever null?
          newFiles = null;
        }
      }

      if (infos != null) {
        TreeLogger.log("top: id=" + id + " commit deleteAll=" + deleteAll + "; infos.version=" + infos.getVersion() + " infos.files=" + newFiles + " segs=" + infos.toString(dir));
        TreeLogger.start("commit");
        dir.sync(newFiles);
        infos.commit(dir);

        String segmentsFileName = infos.getSegmentsFileName();
        deleter.incRef(Collections.singletonList(segmentsFileName));

        synchronized (this) {
          // If a sync happened while we were committing, we
          // must carry over the commit gen:
          SegmentInfos curInfos = mgr.getCurrentInfos();
          if (curInfos != infos) {
            curInfos.updateGeneration(infos);
          }
        }

        TreeLogger.log("top: id=" + id + " " + segmentsFileName + " committed; now decRef lastCommitFiles=" + lastCommitFiles);

        deleter.decRef(lastCommitFiles);
        lastCommitFiles.clear();
        lastCommitFiles.addAll(newFiles);
        lastCommitFiles.add(segmentsFileName);
        TreeLogger.log("id=" + id + " " + infos.getSegmentsFileName() + " lastCommitFiles=" + lastCommitFiles);

        if (deleteAll) {
          // nocommit this is messy: we may delete a merge's files
          // that just copied over as we closed the writer:
          TreeLogger.log("id=" + id + " now deleteUnknownFiles during commit");
          deleter.deleteUnknownFiles();
          TreeLogger.log("id=" + id + " done deleteUnknownFiles during commit");
        }

        TreeLogger.end("commit");
      }
    }
  }

  static class CopyFileJob {
    public final CopyResult result;
    public final Directory src;
    public final FileMetaData metaData;
    public final String fileName;
    public final IOContext context;

    public CopyFileJob(CopyResult result, IOContext context, Directory src, String fileName, FileMetaData metaData) {
      this.result = result;
      this.context = context;
      this.src = src;
      this.fileName = fileName;
      this.metaData = metaData;
    }
  }

  static class CopyOneFile implements Closeable {
    private long bytesLeft;

    // TODO: reuse...?
    private final byte[] buffer = new byte[65536];

    private final IndexInput in;
    private final IndexOutput out;
    private final SlowChecksumDirectory dest;

    public final CopyFileJob job;
    public boolean failed;

    public CopyOneFile(CopyFileJob job, SlowChecksumDirectory dest, RateLimiter rateLimiter) throws IOException {
      this.job = job;
      this.dest = dest;
      in = job.src.openInput(job.fileName, job.context);
      boolean success = false;
      try {
        IndexOutput out0 = dest.createOutput(job.fileName, job.context);
        if (rateLimiter == null) {
          // No IO rate limiting
          out = out0;
        } else {
          out = new RateLimitedIndexOutput(rateLimiter, out0);
        }
        success = true;
      } finally {
        if (success == false) {
          IOUtils.closeWhileHandlingException(in);
        }
      }

      bytesLeft = job.metaData.length;
    }

    public boolean visit() throws IOException {
      int chunk = bytesLeft > buffer.length ? buffer.length : (int) bytesLeft;
      try {
        in.readBytes(buffer, 0, chunk);
        out.writeBytes(buffer, 0, chunk);
      } catch (Exception e) {
        TreeLogger.log("failed to copy " + job.fileName, e);
        failed = true;
      }
      bytesLeft -= chunk;
      return bytesLeft != 0;
    }

    /** Abort the copy. */
    public void abort() {
      TreeLogger.log("now abort copy file " + job.fileName);
      try {
        close();
      } catch (IOException ioe) {
      }
      try {
        dest.deleteFile(job.fileName);
      } catch (IOException ioe) {
      }
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(in, out);
      if (failed) {
        dest.deleteFile(job.fileName);
        throw new IOException("copy failed");
      }

      Long actual = dest.getChecksum(job.fileName);
      assert actual != null;

      if (actual.longValue() != job.metaData.checksum) {
        // Uh oh, bits flipped during copy:
        dest.deleteFile(job.fileName);
        throw new IOException("checksum mismatch");
      }
    }
  }

  static class CopyResult {
    public final CountDownLatch done;
    public final AtomicBoolean failed = new AtomicBoolean();

    public CopyResult(int fileCount) {
      done = new CountDownLatch(fileCount);
    }
  }

  // TODO: abstract this, enable swapping in different
  // low-level tools ... rsync, bittorrent, etc.

  /** Simple class to copy low (merged segments) & high
   *  (flushes) priority files.  It has a simplistic
   *  "ionice" implementation: if we are copying a low-pri
   *  (merge) file and a high-pri (flush) job shows up, then
   *  we pause the low-pri copy and finish all high-pri
   *  copies, then resume it. */

  static class SimpleFileCopier extends Thread implements Closeable {

    private final Queue<CopyFileJob> highPriorityJobs = new ConcurrentLinkedQueue<>();
    private final Queue<CopyFileJob> lowPriorityJobs = new ConcurrentLinkedQueue<>();
    private final SlowChecksumDirectory dest;

    // nocommit make rate limit (10 MB/sec now) controllable:
    private final RateLimiter mergeRateLimiter = new RateLimiter.SimpleRateLimiter(10.0);
    private final int id;
    private boolean stop;

    /** We always copy files into this dest. */
    public SimpleFileCopier(SlowChecksumDirectory dest, int id) {
      this.dest = dest;
      this.id = id;
    }

    // nocommit use Future here
    public synchronized CopyResult add(SlowChecksumDirectory src, Map<String,FileMetaData> files, boolean lowPriority, IOContext context) {
      if (stop) {
        throw new AlreadyClosedException("closed");
      }
      CopyResult result = new CopyResult(files.size());
      Queue<CopyFileJob> queue = lowPriority ? lowPriorityJobs : highPriorityJobs;
      for(Map.Entry<String,FileMetaData> ent : files.entrySet()) {
        queue.add(new CopyFileJob(result, context, src, ent.getKey(), ent.getValue()));
      }
      notify();
      return result;
    }

    @Override
    public void run() {
      Thread.currentThread().setName("fileCopier id=" + id);
      TreeLogger.setLogger(new TreeLogger("fileCopier id=" + id));

      CopyOneFile curLowPri = null;
      CopyOneFile curHighPri = null;

      boolean success = false;

      try {
        while (stop == false) {
          if (curHighPri != null) {
            if (curHighPri.visit() == false) {
              TreeLogger.log("id=" + id + " " + curHighPri.job.fileName + " high-priority copy done");
              // Finished copying; now close & verify checksums:
              try {
                curHighPri.close();
              } catch (IOException ioe) {
                curHighPri.job.result.failed.set(true);
                TreeLogger.log("WARNING: id=" + id + " " + curHighPri.job.fileName + ": failed to copy high-priority file");
              } finally {
                curHighPri.job.result.done.countDown();
              }
              curHighPri = null;
            }
          } else if (highPriorityJobs.isEmpty() == false) {
            // Start new high-priority copy:
            CopyFileJob job = highPriorityJobs.poll();
            try {
              curHighPri = new CopyOneFile(job, dest, null);
              TreeLogger.log("id=" + id + " " + curHighPri.job.fileName + " now start high-priority copy");
            } catch (AlreadyClosedException ace) {
              TreeLogger.log("id=" + id + " " + job.fileName + " skip copy: hit AlreadyClosedException");
              job.result.failed.set(true);
              job.result.done.countDown();
            }
          } else if (curLowPri != null) {
            if (curLowPri.visit() == false) {
              TreeLogger.log("id=" + id + " " + curLowPri.job.fileName + " low-priority copy done");
              // Finished copying; now close & verify checksums:
              try {
                curLowPri.close();
              } catch (IOException ioe) {
                TreeLogger.log("WARNING: id=" + id + " " + curLowPri.job.fileName + ": failed to copy low-priority file");
                curLowPri.job.result.failed.set(true);
              } finally {
                curLowPri.job.result.done.countDown();
              }
              curLowPri = null;
            }
          } else if (lowPriorityJobs.isEmpty() == false) {
            // Start new low-priority copy:
            CopyFileJob job = lowPriorityJobs.poll();
            try {
              curLowPri = new CopyOneFile(job, dest, mergeRateLimiter);
              TreeLogger.log("id=" + id + " " + curLowPri.job.fileName + " now start low-priority copy");
            } catch (AlreadyClosedException ace) {
              TreeLogger.log("id=" + id + " " + job.fileName + " skip copy: hit AlreadyClosedException");
              job.result.failed.set(true);
              job.result.done.countDown();
            }
          } else {
            // Wait for another job:
            synchronized (this) {
              if (highPriorityJobs.isEmpty() && lowPriorityJobs.isEmpty()) {
                TreeLogger.log("id=" + id + " copy thread now idle");
                try {
                  wait();
                } catch (InterruptedException ie) {
                  Thread.currentThread().interrupt();
                }
                TreeLogger.log("id=" + id + " copy thread now wake up");
              }
            }
          }
        }
        success = true;
      } catch (IOException ioe) {
        // nocommit catch each op & retry instead?
        throw new RuntimeException(ioe);
      } finally {
        synchronized(this) {
          stop = true;
          if (curLowPri != null) {
            curLowPri.abort();
            curLowPri.job.result.failed.set(true);
            curLowPri.job.result.done.countDown();
          }
          if (curHighPri != null) {
            curHighPri.abort();
            curHighPri.job.result.failed.set(true);
            curHighPri.job.result.done.countDown();
          }

          for(CopyFileJob job : highPriorityJobs) {
            job.result.failed.set(true);
            job.result.done.countDown();
          }

          for(CopyFileJob job : lowPriorityJobs) {
            job.result.failed.set(true);
            job.result.done.countDown();
          }
        }
      }
    }

    // nocommit cutover all other finishes to closeable

    @Override      
    public synchronized void close() throws IOException {
      stop = true;
      try {
        notify();
        join();
      } catch (InterruptedException ie) {
        // nocommit
        throw new IOException(ie);
      }
    }
  }

  // TODO: we could pre-copy merged files out event before
  // warmMerge is called?  e.g. if we "notice" files being
  // written to the dir ... could give us a "head start"

  /** Runs in each replica, to handle copying over new
   *  flushes. */
  static class CopyThread extends Thread {
    final Lock lock;
    final Condition cond;
    private final Replica replica;
    private volatile boolean stop;

    public CopyThread(Replica replica) {
      this.lock = new ReentrantLock();
      this.cond = lock.newCondition();
      this.stop = false;
      this.replica = replica;
    }

    @Override
    public void run() {
      Thread.currentThread().setName("replica id=" + replica.id);
      TreeLogger.setLogger(new TreeLogger("replica id=" + replica.id));

      try {
        // While loop to keep pulling newly flushed/merged
        // segments until we shutdown
        while (stop == false) {

          try {

            // While loop to pull a single new segment:
            long curVersion = replica.mgr.getCurrentInfos().version;

            SegmentInfos newInfos = null;
            CopyState copyState = null;
            Master curMaster = null;
            int curMasterMoveCount = -1;
            while (stop == false) {
              lock.lock();
              try {
                curMaster = master;
                if (curMaster != null) { 
                  copyState = curMaster.getCopyState();
                  curMasterMoveCount = masterCount.get();
                  if (copyState != null) {
                    assert copyState.version >= curVersion: "copyState.version=" + copyState.version + " vs curVersion=" + curVersion;
                    if (copyState.version > curVersion) {
                      TreeLogger.log("got new copyState");
                      break;
                    } else {
                      TreeLogger.log("skip newInfos");
                      curMaster.releaseCopyState(copyState);
                      copyState = null;
                    }
                  } else {
                    // Master is closed
                    Thread.sleep(5);
                  }
                } else {
                  // Master hasn't started yet
                  Thread.sleep(5);
                }
                cond.awaitUninterruptibly();
              } finally {
                lock.unlock();
              }
            }

            // We hold a ref on newInfos at this point

            // nocommit what if master closes now?

            if (stop) {
              if (copyState != null) {
                curMaster.releaseCopyState(copyState);
              }
              break;
            }

            // nocommit need to sometimes crash during
            // replication; ooh: we could just Thread.interrupt()?

            // nocommit needs to cross the "wire", ie turn into
            // byte[] and back, and manage the "reservation"
            // separately on master

            try {
              // nocommit just pass copyState
              replica.sync(curMasterMoveCount, copyState.dir, copyState.files, copyState.infosBytes, copyState.version);
            } finally {
              curMaster.releaseCopyState(copyState);
            }
          } catch (AlreadyClosedException ace) {
            // OK: master closed while we were replicating;
            // we will just retry again against the next master:
            TreeLogger.log("top: id=" + replica.id + " ignore AlreadyClosedException");
            Thread.sleep(5);
            continue;
          }
        }
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      } catch (IOException ioe) {
        // nocommit how to properly handle...
        throw new RuntimeException(ioe);
      }
    }

    /** Shuts down the thread and only returns once
     *  it's done. */
    public void finish() throws InterruptedException {
      stop = true;
      lock.lock();
      try {
        cond.signal();
      } finally {
        lock.unlock();
      }
      join();
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
              assertNotNull("searcher " + s + " version=" + version + " is missing expected count", expectedCount);
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

        // Necessary in case we had tried to delete this
        // fileName before, it failed, but then it was later
        // overwritten:
        pending.remove(fileName);
      }
    }

    public synchronized void decRef(Collection<String> fileNames) {
      for(String fileName : fileNames) {
        Integer curCount = refCounts.get(fileName);
        assert curCount != null: "fileName=" + fileName;
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
