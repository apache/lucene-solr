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
package org.apache.lucene.index;


import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MockDirectoryWrapper.Throttling;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;

// TODO:
//   - old parallel indices are only pruned on commit/close; can we do it on refresh?

/** Simple example showing how to use ParallelLeafReader to index new
 *  stuff (postings, DVs, etc.) from previously stored fields, on the
 *  fly (during NRT reader reopen), after the  initial indexing.  The
 *  test indexes just a single stored field with text "content X" (X is
 *  a number embedded in the text).
 *
 *  Then, on reopen, for any newly created segments (flush or merge), it
 *  builds a new parallel segment by loading all stored docs, parsing
 *  out that X, and adding it as DV and numeric indexed (trie) field.
 *
 *  Finally, for searching, it builds a top-level MultiReader, with
 *  ParallelLeafReader for each segment, and then tests that random
 *  numeric range queries, and sorting by the new DV field, work
 *  correctly.
 *
 *  Each per-segment index lives in a private directory next to the main
 *  index, and they are deleted once their segments are removed from the
 *  index.  They are "volatile", meaning if e.g. the index is replicated to
 *  another machine, it's OK to not copy parallel segments indices,
 *  since they will just be regnerated (at a cost though). */

// @SuppressSysoutChecks(bugUrl="we print stuff")

public class TestDemoParallelLeafReader extends LuceneTestCase {

  static final boolean DEBUG = false;

  static abstract class ReindexingReader implements Closeable {

    /** Key used to store the current schema gen in the SegmentInfo diagnostics */
    public final static String SCHEMA_GEN_KEY = "schema_gen";

    public final IndexWriter w;
    public final ReaderManager mgr;

    private final Directory indexDir;
    private final Path root;
    private final Path segsPath;

    /** Which segments have been closed, but their parallel index is not yet not removed. */
    private final Set<SegmentIDAndGen> closedSegments = Collections.newSetFromMap(new ConcurrentHashMap<SegmentIDAndGen,Boolean>());

    /** Holds currently open parallel readers for each segment. */
    private final Map<SegmentIDAndGen,LeafReader> parallelReaders = new ConcurrentHashMap<>();

    void printRefCounts() {
      System.out.println("All refCounts:");
      for(Map.Entry<SegmentIDAndGen,LeafReader> ent : parallelReaders.entrySet()) {
        System.out.println("  " + ent.getKey() + " " + ent.getValue() + " refCount=" + ent.getValue().getRefCount());
      }
    }

    public ReindexingReader(Path root) throws IOException {
      this.root = root;

      // Normal index is stored under "index":
      indexDir = openDirectory(root.resolve("index"));

      // Per-segment parallel indices are stored under subdirs "segs":
      segsPath = root.resolve("segs");
      Files.createDirectories(segsPath);

      IndexWriterConfig iwc = getIndexWriterConfig();
      iwc.setMergePolicy(new ReindexingMergePolicy(iwc.getMergePolicy()));
      if (DEBUG) {
        System.out.println("TEST: use IWC:\n" + iwc);
      }
      w = new IndexWriter(indexDir, iwc);

      w.getConfig().setMergedSegmentWarmer(new IndexWriter.IndexReaderWarmer() {
          @Override
          public void warm(LeafReader reader) throws IOException {
            // This will build the parallel index for the merged segment before the merge becomes visible, so reopen delay is only due to
            // newly flushed segments:
            if (DEBUG) System.out.println(Thread.currentThread().getName() +": TEST: now warm " + reader);
            // TODO: it's not great that we pass false here; it means we close the reader & reopen again for NRT reader; still we did "warm" by
            // building the parallel index, if necessary
            getParallelLeafReader(reader, false, getCurrentSchemaGen());
          }
        });

      // start with empty commit:
      w.commit();
      mgr = new ReaderManager(new ParallelLeafDirectoryReader(DirectoryReader.open(w)));
    }

    protected abstract IndexWriterConfig getIndexWriterConfig() throws IOException;

    /** Optional method to validate that the provided parallell reader in fact reflects the changes in schemaGen. */
    protected void checkParallelReader(LeafReader reader, LeafReader parallelReader, long schemaGen) throws IOException {
    }

    /** Override to customize Directory impl. */
    protected Directory openDirectory(Path path) throws IOException {
      return FSDirectory.open(path);
    }

    public void commit() throws IOException {
      w.commit();
    }
    
    LeafReader getCurrentReader(LeafReader reader, long schemaGen) throws IOException {
      LeafReader parallelReader = getParallelLeafReader(reader, true, schemaGen);
      if (parallelReader != null) {

        // We should not be embedding one ParallelLeafReader inside another:
        assertFalse(parallelReader instanceof ParallelLeafReader);
        assertFalse(reader instanceof ParallelLeafReader);

        // NOTE: important that parallelReader is first, so if there are field name overlaps, because changes to the schema
        // overwrote existing field names, it wins:
        LeafReader newReader = new ParallelLeafReader(false, parallelReader, reader) {
          @Override
          public Bits getLiveDocs() {
            return getParallelReaders()[1].getLiveDocs();
          }
          @Override
          public int numDocs() {
            return getParallelReaders()[1].numDocs();
          }
        };

        // Because ParallelLeafReader does its own (extra) incRef:
        parallelReader.decRef();

        return newReader;

      } else {
        // This segment was already current as of currentSchemaGen:
        return reader;
      }
    }

    private class ParallelLeafDirectoryReader extends FilterDirectoryReader {
      public ParallelLeafDirectoryReader(DirectoryReader in) throws IOException {
        super(in, new FilterDirectoryReader.SubReaderWrapper() {
            final long currentSchemaGen = getCurrentSchemaGen();
            @Override
            public LeafReader wrap(LeafReader reader) {
              try {
                return getCurrentReader(reader, currentSchemaGen);
              } catch (IOException ioe) {
                // TODO: must close on exc here:
                throw new RuntimeException(ioe);
              }
            }
          });
      }

      @Override
      protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new ParallelLeafDirectoryReader(in);
      }

      @Override
      protected void doClose() throws IOException {
        Throwable firstExc = null;
        for (final LeafReader r : getSequentialSubReaders()) {
          if (r instanceof ParallelLeafReader) {
            // try to close each reader, even if an exception is thrown
            try {
              r.decRef();
            } catch (Throwable t) {
              if (firstExc == null) {
                firstExc = t;
              }
            }
          }
        }
        // Also close in, so it decRef's the SegmentInfos
        try {
          in.doClose();
        } catch (Throwable t) {
          if (firstExc == null) {
            firstExc = t;
          }
        }
        // throw the first exception
        IOUtils.reThrow(firstExc);
      }
    }

    @Override
    public void close() throws IOException {
      w.close();
      if (DEBUG) System.out.println("TEST: after close writer index=" + SegmentInfos.readLatestCommit(indexDir));

      /*
      DirectoryReader r = mgr.acquire();
      try {
        TestUtil.checkReader(r);
      } finally {
        mgr.release(r);
      }
      */
      mgr.close();
      pruneOldSegments(true);
      assertNoExtraSegments();
      indexDir.close();
    }

    // Make sure we deleted all parallel indices for segments that are no longer in the main index: 
    private void assertNoExtraSegments() throws IOException {
      Set<String> liveIDs = new HashSet<String>();
      for(SegmentCommitInfo info : SegmentInfos.readLatestCommit(indexDir)) {
        String idString = StringHelper.idToString(info.info.getId());
        liveIDs.add(idString);
      }

      // At this point (closing) the only segments in closedSegments should be the still-live ones:
      for(SegmentIDAndGen segIDGen : closedSegments) {
        assertTrue(liveIDs.contains(segIDGen.segID));
      }

      boolean fail = false;
      for(Path path : segSubDirs(segsPath)) {
        SegmentIDAndGen segIDGen = new SegmentIDAndGen(path.getFileName().toString());
        if (liveIDs.contains(segIDGen.segID) == false) {
          if (DEBUG) System.out.println("TEST: fail seg=" + path.getFileName() + " is not live but still has a parallel index");
          fail = true;
        }
      }
      assertFalse(fail);
    }

    private static class SegmentIDAndGen {
      public final String segID;
      public final long schemaGen;

      public SegmentIDAndGen(String segID, long schemaGen) {
        this.segID = segID;
        this.schemaGen = schemaGen;
      }

      public SegmentIDAndGen(String s) {
        String[] parts = s.split("_");
        if (parts.length != 2) {
          throw new IllegalArgumentException("invalid SegmentIDAndGen \"" + s + "\"");
        }
        // TODO: better checking of segID?
        segID = parts[0];
        schemaGen = Long.parseLong(parts[1]);
      }

      @Override
      public int hashCode() {
        return (int) (segID.hashCode() * schemaGen);
      }

      @Override
      public boolean equals(Object _other) {
        if (_other instanceof SegmentIDAndGen) {
          SegmentIDAndGen other = (SegmentIDAndGen) _other;
          return segID.equals(other.segID) && schemaGen == other.schemaGen;
        } else {
          return false;
        }
      }

      @Override
      public String toString() {
        return segID + "_" + schemaGen;
      }
    }

    private class ParallelReaderClosed implements LeafReader.ReaderClosedListener {
      private final SegmentIDAndGen segIDGen;
      private final Directory dir;

      public ParallelReaderClosed(SegmentIDAndGen segIDGen, Directory dir) {
        this.segIDGen = segIDGen;
        this.dir = dir;
      }

      @Override
      public void onClose(IndexReader ignored) {
        try {
          // TODO: make this sync finer, i.e. just the segment + schemaGen
          synchronized(ReindexingReader.this) {
            if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST: now close parallel parLeafReader dir=" + dir + " segIDGen=" + segIDGen);
            parallelReaders.remove(segIDGen);
            dir.close();
            closedSegments.add(segIDGen);
          }
        } catch (IOException ioe) {
          System.out.println("TEST: hit IOExc closing dir=" + dir);
          ioe.printStackTrace(System.out);
          throw new RuntimeException(ioe);
        }
      }
    }

    // Returns a ref
    LeafReader getParallelLeafReader(final LeafReader leaf, boolean doCache, long schemaGen) throws IOException {
      assert leaf instanceof SegmentReader;
      SegmentInfo info = ((SegmentReader) leaf).getSegmentInfo().info;

      long infoSchemaGen = getSchemaGen(info);

      if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST: getParallelLeafReader: " + leaf + " infoSchemaGen=" + infoSchemaGen + " vs schemaGen=" + schemaGen + " doCache=" + doCache);

      if (infoSchemaGen == schemaGen) {
        if (DEBUG) System.out.println(Thread.currentThread().getName()+ ": TEST: segment is already current schemaGen=" + schemaGen + "; skipping");
        return null;
      }

      if (infoSchemaGen > schemaGen) {
        throw new IllegalStateException("segment infoSchemaGen (" + infoSchemaGen + ") cannot be greater than requested schemaGen (" + schemaGen + ")");
      }

      final SegmentIDAndGen segIDGen = new SegmentIDAndGen(StringHelper.idToString(info.getId()), schemaGen);

      // While loop because the parallel reader may be closed out from under us, so we must retry:
      while (true) {

        // TODO: make this sync finer, i.e. just the segment + schemaGen
        synchronized (this) {
          LeafReader parReader = parallelReaders.get(segIDGen);
      
          assert doCache || parReader == null;

          if (parReader == null) {

            Path leafIndex = segsPath.resolve(segIDGen.toString());

            final Directory dir = openDirectory(leafIndex);

            if (slowFileExists(dir, "done") == false) {
              if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST: build segment index for " + leaf + " " + segIDGen + " (source: " + info.getDiagnostics().get("source") + ") dir=" + leafIndex);

              if (dir.listAll().length != 0) {
                // It crashed before finishing last time:
                if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST: remove old incomplete index files: " + leafIndex);
                IOUtils.rm(leafIndex);
              }

              reindex(infoSchemaGen, schemaGen, leaf, dir);

              // Marker file, telling us this index is in fact done.  This way if we crash while doing the reindexing for a given segment, we will
              // later try again:
              dir.createOutput("done", IOContext.DEFAULT).close();
            } else {
              if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST: segment index already exists for " + leaf + " " + segIDGen + " (source: " + info.getDiagnostics().get("source") + ") dir=" + leafIndex);
            }

            if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST: now check index " + dir);
            //TestUtil.checkIndex(dir);

            SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
            assert infos.size() == 1;
            final LeafReader parLeafReader = new SegmentReader(infos.info(0), IOContext.DEFAULT);

            //checkParallelReader(leaf, parLeafReader, schemaGen);

            if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST: opened parallel reader: " + parLeafReader);
            if (doCache) {
              parallelReaders.put(segIDGen, parLeafReader);

              // Our id+gen could have been previously closed, e.g. if it was a merged segment that was warmed, so we must clear this else
              // the pruning may remove our directory:
              closedSegments.remove(segIDGen);

              parLeafReader.addReaderClosedListener(new ParallelReaderClosed(segIDGen, dir));

            } else {
              // Used only for merged segment warming:
              // Messy: we close this reader now, instead of leaving open for reuse:
              if (DEBUG) System.out.println("TEST: now decRef non cached refCount=" + parLeafReader.getRefCount());
              parLeafReader.decRef();
              dir.close();

              // Must do this after dir is closed, else another thread could "rm -rf" while we are closing (which makes MDW.close's
              // checkIndex angry):
              closedSegments.add(segIDGen);
              parReader = null;
            }
            parReader = parLeafReader;

          } else {
            if (parReader.tryIncRef() == false) {
              // We failed: this reader just got closed by another thread, e.g. refresh thread opening a new reader, so this reader is now
              // closed and we must try again.
              if (DEBUG) System.out.println(Thread.currentThread().getName()+ ": TEST: tryIncRef failed for " + parReader + "; retry");
              parReader = null;
              continue;
            }
            if (DEBUG) System.out.println(Thread.currentThread().getName()+ ": TEST: use existing already opened parReader=" + parReader + " refCount=" + parReader.getRefCount());
            //checkParallelReader(leaf, parReader, schemaGen);
          }

          // We return the new reference to caller
          return parReader;
        }
      }
    }

    // TODO: we could pass a writer already opened...?
    protected abstract void reindex(long oldSchemaGen, long newSchemaGen, LeafReader reader, Directory parallelDir) throws IOException;

    /** Returns the gen for the current schema. */
    protected abstract long getCurrentSchemaGen();

    /** Returns the gen that should be merged, meaning those changes will be folded back into the main index. */
    protected long getMergingSchemaGen() {
      return getCurrentSchemaGen();
    }

    /** Removes the parallel index that are no longer in the last commit point.  We can't
     *  remove this when the parallel reader is closed because it may still be referenced by
     *  the last commit. */
    private void pruneOldSegments(boolean removeOldGens) throws IOException {
      SegmentInfos lastCommit = SegmentInfos.readLatestCommit(indexDir);
      if (DEBUG) System.out.println("TEST: prune");

      Set<String> liveIDs = new HashSet<String>();
      for(SegmentCommitInfo info : lastCommit) {
        String idString = StringHelper.idToString(info.info.getId());
        liveIDs.add(idString);
      }

      long currentSchemaGen = getCurrentSchemaGen();

      if (Files.exists(segsPath)) {
        for (Path path : segSubDirs(segsPath)) {
          if (Files.isDirectory(path)) {
            SegmentIDAndGen segIDGen = new SegmentIDAndGen(path.getFileName().toString());
            assert segIDGen.schemaGen <= currentSchemaGen;
            if (liveIDs.contains(segIDGen.segID) == false && (closedSegments.contains(segIDGen) || (removeOldGens && segIDGen.schemaGen < currentSchemaGen))) {
              if (DEBUG) System.out.println("TEST: remove " + segIDGen);
              try {
                IOUtils.rm(path);
                closedSegments.remove(segIDGen);
              } catch (IOException ioe) {
                // OK, we'll retry later
                if (DEBUG) System.out.println("TEST: ignore ioe during delete " + path + ":" + ioe);
              }
            }
          }
        }
      }
    }

    /** Just replaces the sub-readers with parallel readers, so reindexed fields are merged into new segments. */
    private class ReindexingMergePolicy extends MergePolicyWrapper {

      class ReindexingOneMerge extends OneMerge {

        final List<ParallelLeafReader> parallelReaders = new ArrayList<>();
        final long schemaGen;

        ReindexingOneMerge(List<SegmentCommitInfo> segments) {
          super(segments);
          // Commit up front to which schemaGen we will merge; we don't want a schema change sneaking in for some of our leaf readers but not others:
          schemaGen = getMergingSchemaGen();
          long currentSchemaGen = getCurrentSchemaGen();

          // Defensive sanity check:
          if (schemaGen > currentSchemaGen) {
            throw new IllegalStateException("currentSchemaGen (" + currentSchemaGen + ") must always be >= mergingSchemaGen (" + schemaGen + ")");
          }
        }

        @Override
        public CodecReader wrapForMerge(CodecReader reader) throws IOException {
          LeafReader wrapped = getCurrentReader((SegmentReader)reader, schemaGen);
          if (wrapped instanceof ParallelLeafReader) {
            parallelReaders.add((ParallelLeafReader) wrapped);
          }
          return SlowCodecReaderWrapper.wrap(wrapped);
        }

        @Override
        public void mergeFinished() throws IOException {
          Throwable th = null;
          for (ParallelLeafReader r : parallelReaders) {
            try {
              r.decRef();
            } catch (Throwable t) {
              if (th == null) {
                th = t;
              }
            }
          }

          // If any error occured, throw it.
          IOUtils.reThrow(th);
        }
    
        @Override
        public void setMergeInfo(SegmentCommitInfo info) {
          // Record that this merged segment is current as of this schemaGen:
          info.info.getDiagnostics().put(SCHEMA_GEN_KEY, Long.toString(schemaGen));
          super.setMergeInfo(info);
        }

      }

      class ReindexingMergeSpecification extends MergeSpecification {
        @Override
        public void add(OneMerge merge) {
          super.add(new ReindexingOneMerge(merge.segments));
        }

        @Override
        public String segString(Directory dir) {
          return "ReindexingMergeSpec(" + super.segString(dir) + ")";
        }
      }

      MergeSpecification wrap(MergeSpecification spec) {
        MergeSpecification wrapped = null;
        if (spec != null) {
          wrapped = new ReindexingMergeSpecification();
          for (OneMerge merge : spec.merges) {
            wrapped.add(merge);
          }
        }
        return wrapped;
      }

      /** Create a new {@code MergePolicy} that sorts documents with the given {@code sort}. */
      public ReindexingMergePolicy(MergePolicy in) {
        super(in);
      }

      @Override
      public MergeSpecification findMerges(MergeTrigger mergeTrigger,
                                           SegmentInfos segmentInfos, IndexWriter writer) throws IOException {
        return wrap(in.findMerges(mergeTrigger, segmentInfos, writer));
      }

      @Override
      public MergeSpecification findForcedMerges(SegmentInfos segmentInfos,
                                                 int maxSegmentCount, Map<SegmentCommitInfo,Boolean> segmentsToMerge, IndexWriter writer)
        throws IOException {
        // TODO: do we need to force-force this?  Ie, wrapped MP may think index is already optimized, yet maybe its schemaGen is old?  need test!
        return wrap(in.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, writer));
      }

      @Override
      public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, IndexWriter writer)
        throws IOException {
        return wrap(in.findForcedDeletesMerges(segmentInfos, writer));
      }

      @Override
      public boolean useCompoundFile(SegmentInfos segments,
                                     SegmentCommitInfo newSegment, IndexWriter writer) throws IOException {
        return in.useCompoundFile(segments, newSegment, writer);
      }

      @Override
      public String toString() {
        return "ReindexingMergePolicy(" + in + ")";
      }
    }

    static long getSchemaGen(SegmentInfo info) {
      String s = info.getDiagnostics().get(SCHEMA_GEN_KEY);
      if (s == null) {
        return -1;
      } else {
        return Long.parseLong(s);
      }
    }
  }

  private ReindexingReader getReindexer(Path root) throws IOException {
    return new ReindexingReader(root) {
      @Override
      protected IndexWriterConfig getIndexWriterConfig() throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig();
        TieredMergePolicy tmp = new TieredMergePolicy();
        // We write tiny docs, so we need tiny floor to avoid O(N^2) merging:
        tmp.setFloorSegmentMB(.01);
        iwc.setMergePolicy(tmp);
        return iwc;
      }

      @Override
      protected Directory openDirectory(Path path) throws IOException {
        MockDirectoryWrapper dir = newMockFSDirectory(path);
        dir.setUseSlowOpenClosers(false);
        dir.setThrottling(Throttling.NEVER);
        return dir;
      }

      @Override
      protected void reindex(long oldSchemaGen, long newSchemaGen, LeafReader reader, Directory parallelDir) throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig();

        // The order of our docIDs must precisely matching incoming reader:
        iwc.setMergePolicy(new LogByteSizeMergePolicy());
        IndexWriter w = new IndexWriter(parallelDir, iwc);
        int maxDoc = reader.maxDoc();

        // Slowly parse the stored field into a new doc values field:
        for(int i=0;i<maxDoc;i++) {
          // TODO: is this still O(blockSize^2)?
          Document oldDoc = reader.document(i);
          Document newDoc = new Document();
          long value = Long.parseLong(oldDoc.get("text").split(" ")[1]);
          newDoc.add(new NumericDocValuesField("number", value));
          newDoc.add(new LongPoint("number", value));
          w.addDocument(newDoc);
        }

        w.forceMerge(1);

        w.close();
      }

      @Override
      protected long getCurrentSchemaGen() {
        return 0;
      }
    };
  }

  /** Schema change by adding a new number_<schemaGen> DV field each time. */
  private ReindexingReader getReindexerNewDVFields(Path root, final AtomicLong currentSchemaGen) throws IOException {
    return new ReindexingReader(root) {
      @Override
      protected IndexWriterConfig getIndexWriterConfig() throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig();
        TieredMergePolicy tmp = new TieredMergePolicy();
        // We write tiny docs, so we need tiny floor to avoid O(N^2) merging:
        tmp.setFloorSegmentMB(.01);
        iwc.setMergePolicy(tmp);
        return iwc;
      }

      @Override
      protected Directory openDirectory(Path path) throws IOException {
        MockDirectoryWrapper dir = newMockFSDirectory(path);
        dir.setUseSlowOpenClosers(false);
        dir.setThrottling(Throttling.NEVER);
        return dir;
      }

      @Override
      protected void reindex(long oldSchemaGen, long newSchemaGen, LeafReader reader, Directory parallelDir) throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig();

        // The order of our docIDs must precisely matching incoming reader:
        iwc.setMergePolicy(new LogByteSizeMergePolicy());
        IndexWriter w = new IndexWriter(parallelDir, iwc);
        int maxDoc = reader.maxDoc();

        if (oldSchemaGen <= 0) {
          // Must slowly parse the stored field into a new doc values field:
          for(int i=0;i<maxDoc;i++) {
            // TODO: is this still O(blockSize^2)?
            Document oldDoc = reader.document(i);
            Document newDoc = new Document();
            long value = Long.parseLong(oldDoc.get("text").split(" ")[1]);
            newDoc.add(new NumericDocValuesField("number_" + newSchemaGen, value));
            newDoc.add(new LongPoint("number", value));
            w.addDocument(newDoc);
          }
        } else {
          // Just carry over doc values from previous field:
          NumericDocValues oldValues = reader.getNumericDocValues("number_" + oldSchemaGen);
          assertNotNull("oldSchemaGen=" + oldSchemaGen, oldValues);
          for(int i=0;i<maxDoc;i++) {
            // TODO: is this still O(blockSize^2)?
            Document oldDoc = reader.document(i);
            Document newDoc = new Document();
            newDoc.add(new NumericDocValuesField("number_" + newSchemaGen, oldValues.get(i)));
            w.addDocument(newDoc);
          }
        }

        w.forceMerge(1);

        w.close();
      }

      @Override
      protected long getCurrentSchemaGen() {
        return currentSchemaGen.get();
      }

      @Override
      protected void checkParallelReader(LeafReader r, LeafReader parR, long schemaGen) throws IOException {
        String fieldName = "number_" + schemaGen;
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST: now check parallel number DVs field=" + fieldName + " r=" + r + " parR=" + parR);
        NumericDocValues numbers = parR.getNumericDocValues(fieldName);
        if (numbers == null) {
          return;
        }
        int maxDoc = r.maxDoc();
        boolean failed = false;
        for(int i=0;i<maxDoc;i++) {
          Document oldDoc = r.document(i);
          long value = Long.parseLong(oldDoc.get("text").split(" ")[1]);
          if (value != numbers.get(i)) {
            if (DEBUG) System.out.println("FAIL: docID=" + i + " " + oldDoc+ " value=" + value + " number=" + numbers.get(i) + " numbers=" + numbers);
            failed = true;
          } else if (failed) {
            if (DEBUG) System.out.println("OK: docID=" + i + " " + oldDoc+ " value=" + value + " number=" + numbers.get(i));
          }
        }
        assertFalse("FAILED field=" + fieldName + " r=" + r, failed);
      }
    };
  }

  /** Schema change by adding changing how the same "number" DV field is indexed. */
  private ReindexingReader getReindexerSameDVField(Path root, final AtomicLong currentSchemaGen, final AtomicLong mergingSchemaGen) throws IOException {
    return new ReindexingReader(root) {
      @Override
      protected IndexWriterConfig getIndexWriterConfig() throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig();
        TieredMergePolicy tmp = new TieredMergePolicy();
        // We write tiny docs, so we need tiny floor to avoid O(N^2) merging:
        tmp.setFloorSegmentMB(.01);
        iwc.setMergePolicy(tmp);
        if (TEST_NIGHTLY) {
          // during nightly tests, we might use too many files if we arent careful
          iwc.setUseCompoundFile(true);
        }
        return iwc;
      }

      @Override
      protected Directory openDirectory(Path path) throws IOException {
        MockDirectoryWrapper dir = newMockFSDirectory(path);
        dir.setUseSlowOpenClosers(false);
        dir.setThrottling(Throttling.NEVER);
        return dir;
      }

      @Override
      protected void reindex(long oldSchemaGen, long newSchemaGen, LeafReader reader, Directory parallelDir) throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig();

        // The order of our docIDs must precisely matching incoming reader:
        iwc.setMergePolicy(new LogByteSizeMergePolicy());
        IndexWriter w = new IndexWriter(parallelDir, iwc);
        int maxDoc = reader.maxDoc();

        if (oldSchemaGen <= 0) {
          // Must slowly parse the stored field into a new doc values field:
          for(int i=0;i<maxDoc;i++) {
            // TODO: is this still O(blockSize^2)?
            Document oldDoc = reader.document(i);
            Document newDoc = new Document();
            long value = Long.parseLong(oldDoc.get("text").split(" ")[1]);
            newDoc.add(new NumericDocValuesField("number", newSchemaGen*value));
            newDoc.add(new LongPoint("number", value));
            w.addDocument(newDoc);
          }
        } else {
          // Just carry over doc values from previous field:
          NumericDocValues oldValues = reader.getNumericDocValues("number");
          assertNotNull("oldSchemaGen=" + oldSchemaGen, oldValues);
          for(int i=0;i<maxDoc;i++) {
            // TODO: is this still O(blockSize^2)?
            Document oldDoc = reader.document(i);
            Document newDoc = new Document();
            newDoc.add(new NumericDocValuesField("number", newSchemaGen*(oldValues.get(i)/oldSchemaGen)));
            w.addDocument(newDoc);
          }
        }

        w.forceMerge(1);

        w.close();
      }

      @Override
      protected long getCurrentSchemaGen() {
        return currentSchemaGen.get();
      }

      @Override
      protected long getMergingSchemaGen() {
        return mergingSchemaGen.get();
      }

      @Override
      protected void checkParallelReader(LeafReader r, LeafReader parR, long schemaGen) throws IOException {
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST: now check parallel number DVs r=" + r + " parR=" + parR);
        NumericDocValues numbers = parR.getNumericDocValues("numbers");
        if (numbers == null) {
          return;
        }
        int maxDoc = r.maxDoc();
        boolean failed = false;
        for(int i=0;i<maxDoc;i++) {
          Document oldDoc = r.document(i);
          long value = Long.parseLong(oldDoc.get("text").split(" ")[1]);
          value *= schemaGen;
          if (value != numbers.get(i)) {
            System.out.println("FAIL: docID=" + i + " " + oldDoc+ " value=" + value + " number=" + numbers.get(i) + " numbers=" + numbers);
            failed = true;
          } else if (failed) {
            System.out.println("OK: docID=" + i + " " + oldDoc+ " value=" + value + " number=" + numbers.get(i));
          }
        }
        assertFalse("FAILED r=" + r, failed);
      }
    };
  }

  public void testBasicMultipleSchemaGens() throws Exception {

    AtomicLong currentSchemaGen = new AtomicLong();

    // TODO: separate refresh thread, search threads, indexing threads
    Path root = createTempDir();
    ReindexingReader reindexer = getReindexerNewDVFields(root, currentSchemaGen);
    reindexer.commit();

    Document doc = new Document();
    doc.add(newTextField("text", "number " + random().nextLong(), Field.Store.YES));
    reindexer.w.addDocument(doc);

    if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST: refresh @ 1 doc");
    reindexer.mgr.maybeRefresh();
    DirectoryReader r = reindexer.mgr.acquire();
    if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST: got reader=" + r);
    try {
      checkAllNumberDVs(r, "number_" + currentSchemaGen.get(), true, 1);
    } finally {
      reindexer.mgr.release(r);
    }
    //reindexer.printRefCounts();

    currentSchemaGen.incrementAndGet();

    if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST: increment schemaGen");
    if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST: commit");
    reindexer.commit();

    doc = new Document();
    doc.add(newTextField("text", "number " + random().nextLong(), Field.Store.YES));
    reindexer.w.addDocument(doc);

    if (DEBUG) System.out.println("TEST: refresh @ 2 docs");
    reindexer.mgr.maybeRefresh();
    //reindexer.printRefCounts();
    r = reindexer.mgr.acquire();
    if (DEBUG) System.out.println("TEST: got reader=" + r);
    try {
      checkAllNumberDVs(r, "number_" + currentSchemaGen.get(), true, 1);
    } finally {
      reindexer.mgr.release(r);
    }

    if (DEBUG) System.out.println("TEST: forceMerge");
    reindexer.w.forceMerge(1);

    currentSchemaGen.incrementAndGet();

    if (DEBUG) System.out.println("TEST: commit");
    reindexer.commit();

    if (DEBUG) System.out.println("TEST: refresh after forceMerge");
    reindexer.mgr.maybeRefresh();
    r = reindexer.mgr.acquire();
    if (DEBUG) System.out.println("TEST: got reader=" + r);
    try {
      checkAllNumberDVs(r, "number_" + currentSchemaGen.get(), true, 1);
    } finally {
      reindexer.mgr.release(r);
    }

    if (DEBUG) System.out.println("TEST: close writer");
    reindexer.close();
  }

  public void testRandomMultipleSchemaGens() throws Exception {

    AtomicLong currentSchemaGen = new AtomicLong();
    ReindexingReader reindexer = null;

    // TODO: separate refresh thread, search threads, indexing threads
    int numDocs = atLeast(TEST_NIGHTLY ? 20000 : 1000);
    int maxID = 0;
    Path root = createTempDir();
    int refreshEveryNumDocs = 100;
    int commitCloseNumDocs = 1000;
    for(int i=0;i<numDocs;i++) {
      if (reindexer == null) {
        reindexer = getReindexerNewDVFields(root, currentSchemaGen);
      }

      Document doc = new Document();
      String id;
      String updateID;
      if (maxID > 0 && random().nextInt(10) == 7) {
        // Replace a doc
        id = "" + random().nextInt(maxID);
        updateID = id;
      } else {
        id = "" + (maxID++);
        updateID = null;
      }
        
      doc.add(newStringField("id", id, Field.Store.NO));
      doc.add(newTextField("text", "number " + random().nextLong(), Field.Store.YES));
      if (updateID == null) {
        reindexer.w.addDocument(doc);
      } else {
        reindexer.w.updateDocument(new Term("id", updateID), doc);
      }
      if (random().nextInt(refreshEveryNumDocs) == 17) {
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: refresh @ " + (i+1) + " docs");
        reindexer.mgr.maybeRefresh();

        DirectoryReader r = reindexer.mgr.acquire();
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: got reader=" + r);
        try {
          checkAllNumberDVs(r, "number_" + currentSchemaGen.get(), true, 1);
        } finally {
          reindexer.mgr.release(r);
        }
        if (DEBUG) reindexer.printRefCounts();
        refreshEveryNumDocs = (int) (1.25 * refreshEveryNumDocs);
      }

      if (random().nextInt(500) == 17) {
        currentSchemaGen.incrementAndGet();
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: advance schemaGen to " + currentSchemaGen);
      }

      if (i > 0 && random().nextInt(10) == 7) {
        // Random delete:
        reindexer.w.deleteDocuments(new Term("id", ""+random().nextInt(i)));
      }

      if (random().nextInt(commitCloseNumDocs) == 17) {
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: commit @ " + (i+1) + " docs");
        reindexer.commit();
        //reindexer.printRefCounts();
        commitCloseNumDocs = (int) (1.25 * commitCloseNumDocs);
      }

      // Sometimes close & reopen writer/manager, to confirm the parallel segments persist:
      if (random().nextInt(commitCloseNumDocs) == 17) {
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: close writer @ " + (i+1) + " docs");
        reindexer.close();
        reindexer = null;
        commitCloseNumDocs = (int) (1.25 * commitCloseNumDocs);
      }
    }

    if (reindexer != null) {
      reindexer.close();
    }
  }

  /** First schema change creates a new "number" DV field off the stored field; subsequent changes just change the value of that number
   *  field for all docs. */
  public void testRandomMultipleSchemaGensSameField() throws Exception {

    AtomicLong currentSchemaGen = new AtomicLong();
    AtomicLong mergingSchemaGen = new AtomicLong();

    ReindexingReader reindexer = null;

    // TODO: separate refresh thread, search threads, indexing threads
    int numDocs = atLeast(TEST_NIGHTLY ? 20000 : 1000);
    int maxID = 0;
    Path root = createTempDir();
    int refreshEveryNumDocs = 100;
    int commitCloseNumDocs = 1000;

    for(int i=0;i<numDocs;i++) {
      if (reindexer == null) {
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: open new reader/writer");
        reindexer = getReindexerSameDVField(root, currentSchemaGen, mergingSchemaGen);
      }

      Document doc = new Document();
      String id;
      String updateID;
      if (maxID > 0 && random().nextInt(10) == 7) {
        // Replace a doc
        id = "" + random().nextInt(maxID);
        updateID = id;
      } else {
        id = "" + (maxID++);
        updateID = null;
      }
        
      doc.add(newStringField("id", id, Field.Store.NO));
      doc.add(newTextField("text", "number " + TestUtil.nextInt(random(), -10000, 10000), Field.Store.YES));
      if (updateID == null) {
        reindexer.w.addDocument(doc);
      } else {
        reindexer.w.updateDocument(new Term("id", updateID), doc);
      }
      if (random().nextInt(refreshEveryNumDocs) == 17) {
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: refresh @ " + (i+1) + " docs");
        reindexer.mgr.maybeRefresh();
        DirectoryReader r = reindexer.mgr.acquire();
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: got reader=" + r);
        try {
          checkAllNumberDVs(r, "number", true, (int) currentSchemaGen.get());
        } finally {
          reindexer.mgr.release(r);
        }
        if (DEBUG) reindexer.printRefCounts();
        refreshEveryNumDocs = (int) (1.25 * refreshEveryNumDocs);
      }

      if (random().nextInt(500) == 17) {
        currentSchemaGen.incrementAndGet();
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: advance schemaGen to " + currentSchemaGen);
        if (random().nextBoolean()) {
          mergingSchemaGen.incrementAndGet();
          if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: advance mergingSchemaGen to " + mergingSchemaGen);
        }
      }

      if (i > 0 && random().nextInt(10) == 7) {
        // Random delete:
        reindexer.w.deleteDocuments(new Term("id", ""+random().nextInt(i)));
      }

      if (random().nextInt(commitCloseNumDocs) == 17) {
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: commit @ " + (i+1) + " docs");
        reindexer.commit();
        //reindexer.printRefCounts();
        commitCloseNumDocs = (int) (1.25 * commitCloseNumDocs);
      }

      // Sometimes close & reopen writer/manager, to confirm the parallel segments persist:
      if (random().nextInt(commitCloseNumDocs) == 17) {
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: close writer @ " + (i+1) + " docs");
        reindexer.close();
        reindexer = null;
        commitCloseNumDocs = (int) (1.25 * commitCloseNumDocs);
      }
    }

    if (reindexer != null) {
      reindexer.close();
    }

    // Verify main index never reflects schema changes beyond mergingSchemaGen:
    try (Directory dir = newFSDirectory(root.resolve("index"));
         IndexReader r = DirectoryReader.open(dir)) {
        for (LeafReaderContext ctx : r.leaves()) {
          LeafReader leaf = ctx.reader();
          NumericDocValues numbers = leaf.getNumericDocValues("number");
          if (numbers != null) {
            int maxDoc = leaf.maxDoc();
            for(int i=0;i<maxDoc;i++) {
              Document doc = leaf.document(i);
              long value = Long.parseLong(doc.get("text").split(" ")[1]);
              long dvValue = numbers.get(i);
              if (value == 0) {
                assertEquals(0, dvValue);
              } else {
                assertTrue(dvValue % value == 0);
                assertTrue(dvValue / value <= mergingSchemaGen.get());
              }
            }
          }
        }
      }
  }

  public void testBasic() throws Exception {
    Path tempPath = createTempDir();
    ReindexingReader reindexer = getReindexer(tempPath);

    // Start with initial empty commit:
    reindexer.commit();

    Document doc = new Document();
    doc.add(newTextField("text", "number " + random().nextLong(), Field.Store.YES));
    reindexer.w.addDocument(doc);

    if (DEBUG) System.out.println("TEST: refresh @ 1 doc");
    reindexer.mgr.maybeRefresh();
    DirectoryReader r = reindexer.mgr.acquire();
    if (DEBUG) System.out.println("TEST: got reader=" + r);
    try {
      checkAllNumberDVs(r);
      IndexSearcher s = newSearcher(r);
      testNumericDVSort(s);
      testPointRangeQuery(s);
    } finally {
      reindexer.mgr.release(r);
    }
    //reindexer.printRefCounts();

    if (DEBUG) System.out.println("TEST: commit");
    reindexer.commit();

    doc = new Document();
    doc.add(newTextField("text", "number " + random().nextLong(), Field.Store.YES));
    reindexer.w.addDocument(doc);

    if (DEBUG) System.out.println("TEST: refresh @ 2 docs");
    reindexer.mgr.maybeRefresh();
    //reindexer.printRefCounts();
    r = reindexer.mgr.acquire();
    if (DEBUG) System.out.println("TEST: got reader=" + r);
    try {
      checkAllNumberDVs(r);
      IndexSearcher s = newSearcher(r);
      testNumericDVSort(s);
      testPointRangeQuery(s);
    } finally {
      reindexer.mgr.release(r);
    }

    if (DEBUG) System.out.println("TEST: forceMerge");
    reindexer.w.forceMerge(1);

    if (DEBUG) System.out.println("TEST: commit");
    reindexer.commit();

    if (DEBUG) System.out.println("TEST: refresh after forceMerge");
    reindexer.mgr.maybeRefresh();
    r = reindexer.mgr.acquire();
    if (DEBUG) System.out.println("TEST: got reader=" + r);
    try {
      checkAllNumberDVs(r);
      IndexSearcher s = newSearcher(r);
      testNumericDVSort(s);
      testPointRangeQuery(s);
    } finally {
      reindexer.mgr.release(r);
    }

    if (DEBUG) System.out.println("TEST: close writer");
    reindexer.close();
  }

  public void testRandom() throws Exception {
    Path root = createTempDir();
    ReindexingReader reindexer = null;

    // TODO: separate refresh thread, search threads, indexing threads
    int numDocs = atLeast(TEST_NIGHTLY ? 20000 : 1000);
    int maxID = 0;
    int refreshEveryNumDocs = 100;
    int commitCloseNumDocs = 1000;
    for(int i=0;i<numDocs;i++) {
      if (reindexer == null) {
        reindexer = getReindexer(root);
      }

      Document doc = new Document();
      String id;
      String updateID;
      if (maxID > 0 && random().nextInt(10) == 7) {
        // Replace a doc
        id = "" + random().nextInt(maxID);
        updateID = id;
      } else {
        id = "" + (maxID++);
        updateID = null;
      }
        
      doc.add(newStringField("id", id, Field.Store.NO));
      doc.add(newTextField("text", "number " + random().nextLong(), Field.Store.YES));
      if (updateID == null) {
        reindexer.w.addDocument(doc);
      } else {
        reindexer.w.updateDocument(new Term("id", updateID), doc);
      }

      if (random().nextInt(refreshEveryNumDocs) == 17) {
        if (DEBUG) System.out.println("TEST: refresh @ " + (i+1) + " docs");
        reindexer.mgr.maybeRefresh();
        DirectoryReader r = reindexer.mgr.acquire();
        if (DEBUG) System.out.println("TEST: got reader=" + r);
        try {
          checkAllNumberDVs(r);
          IndexSearcher s = newSearcher(r);
          testNumericDVSort(s);
          testPointRangeQuery(s);
        } finally {
          reindexer.mgr.release(r);
        }
        refreshEveryNumDocs = (int) (1.25 * refreshEveryNumDocs);
      }

      if (i > 0 && random().nextInt(10) == 7) {
        // Random delete:
        reindexer.w.deleteDocuments(new Term("id", ""+random().nextInt(i)));
      }

      if (random().nextInt(commitCloseNumDocs) == 17) {
        if (DEBUG) System.out.println("TEST: commit @ " + (i+1) + " docs");
        reindexer.commit();
        commitCloseNumDocs = (int) (1.25 * commitCloseNumDocs);
      }

      // Sometimes close & reopen writer/manager, to confirm the parallel segments persist:
      if (random().nextInt(commitCloseNumDocs) == 17) {
        if (DEBUG) System.out.println("TEST: close writer @ " + (i+1) + " docs");
        reindexer.close();
        reindexer = null;
        commitCloseNumDocs = (int) (1.25 * commitCloseNumDocs);
      }
    }
    if (reindexer != null) {
      reindexer.close();
    }
  }

  private static void checkAllNumberDVs(IndexReader r) throws IOException {
    checkAllNumberDVs(r, "number", true, 1);
  }

  private static void checkAllNumberDVs(IndexReader r, String fieldName, boolean doThrow, int multiplier) throws IOException {
    NumericDocValues numbers = MultiDocValues.getNumericValues(r, fieldName);
    int maxDoc = r.maxDoc();
    boolean failed = false;
    long t0 = System.currentTimeMillis();
    for(int i=0;i<maxDoc;i++) {
      Document oldDoc = r.document(i);
      long value = multiplier * Long.parseLong(oldDoc.get("text").split(" ")[1]);
      if (value != numbers.get(i)) {
        System.out.println("FAIL: docID=" + i + " " + oldDoc+ " value=" + value + " number=" + numbers.get(i) + " numbers=" + numbers);
        failed = true;
      } else if (failed) {
        System.out.println("OK: docID=" + i + " " + oldDoc+ " value=" + value + " number=" + numbers.get(i));
      }
    }
    if (failed) {
      if (r instanceof LeafReader == false) {
        System.out.println("TEST FAILED; check leaves");
        for(LeafReaderContext ctx : r.leaves()) {
          System.out.println("CHECK LEAF=" + ctx.reader());
          checkAllNumberDVs(ctx.reader(), fieldName, false, 1);
        }
      }
      if (doThrow) {
        assertFalse("FAILED field=" + fieldName + " r=" + r, failed);
      } else {
        System.out.println("FAILED field=" + fieldName + " r=" + r);
      }
    }
  }

  private static void testNumericDVSort(IndexSearcher s) throws IOException {
    // Confirm we can sort by the new DV field:
    TopDocs hits = s.search(new MatchAllDocsQuery(), 100, new Sort(new SortField("number", SortField.Type.LONG)));
    NumericDocValues numbers = MultiDocValues.getNumericValues(s.getIndexReader(), "number");
    long last = Long.MIN_VALUE;
    for(ScoreDoc scoreDoc : hits.scoreDocs) {
      long value = Long.parseLong(s.doc(scoreDoc.doc).get("text").split(" ")[1]);
      assertTrue(value >= last);
      assertEquals(value, numbers.get(scoreDoc.doc));
      last = value;
    }
  }

  private static void testPointRangeQuery(IndexSearcher s) throws IOException {
    NumericDocValues numbers = MultiDocValues.getNumericValues(s.getIndexReader(), "number");
    for(int i=0;i<100;i++) {
      // Confirm we can range search by the new indexed (numeric) field:
      long min = random().nextLong();
      long max = random().nextLong();
      if (min > max) {
        long x = min;
        min = max;
        max = x;
      }

      TopDocs hits = s.search(LongPoint.newRangeQuery("number", min, max), 100);
      for(ScoreDoc scoreDoc : hits.scoreDocs) {
        long value = Long.parseLong(s.doc(scoreDoc.doc).get("text").split(" ")[1]);
        assertTrue(value >= min);
        assertTrue(value <= max);
        assertEquals(value, numbers.get(scoreDoc.doc));
      }
    }
  }

  // TODO: maybe the leading id could be further restricted?  It's from StringHelper.idToString:
  static final Pattern SEG_GEN_SUB_DIR_PATTERN = Pattern.compile("^[a-z0-9]+_([0-9]+)$");

  private static List<Path> segSubDirs(Path segsPath) throws IOException {
    List<Path> result = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(segsPath)) {
      for (Path path : stream) {
        // Must be form <segIDString>_<longGen>
        if (Files.isDirectory(path) && SEG_GEN_SUB_DIR_PATTERN.matcher(path.getFileName().toString()).matches()) {
          result.add(path);
        }
      }
    }

    return result;
  }

  // TODO: test exceptions
}
