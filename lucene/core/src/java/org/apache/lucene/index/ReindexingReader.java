package org.apache.lucene.index;

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

import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;

// TODO: how to use FieldTypes to give a higher level API here?

/** Provides a {@link ReaderManager} allowing you to also make "parallel"
 *  changes to the previously indexed documents, e.g. turning stored
 *  fields into doc values, changing norm values, etc.
 *
 *  <p>
 *  This uses ParallelLeafReader to index new
 *  stuff (postings, DVs, etc.) from previously stored fields, on the
 *  fly (during NRT reader reopen), after the  initial indexing.  The
 *  test indexes just a single stored field with text "content X" (X is
 *  a number embedded in the text).
 *
 *  <p>
 *  Then, on reopen, for any newly created segments (flush or merge), it
 *  builds a new parallel segment by loading all stored docs, parsing
 *  out that X, and adding it as DV and numeric indexed (trie) field.
 *
 *  <p>
 *  Finally, for searching, it builds a top-level MultiReader, with
 *  ParallelLeafReader for each segment, and then tests that random
 *  numeric range queries, and sorting by the new DV field, work
 *  correctly.
 *
 *  <p>
 *  Each per-segment index lives in a private directory next to the main
 *  index, and they are deleted once their segments are removed from the
 *  index.  They are "volatile", meaning if e.g. the index is replicated to
 *  another machine, it's OK to not copy parallel segments indices,
 *  since they will just be regnerated (at a cost though). */
public abstract class ReindexingReader implements Closeable {

  private static boolean DEBUG = false;

  /** Key used to store the current schema gen in the SegmentInfo diagnostics */
  public final static String SCHEMA_GEN_KEY = "schema_gen";

  public final IndexWriter w;
  public final ReaderManager mgr;

  // Main index directory:
  public final Directory indexDir;

  // Parent directory holding sub directory index for each segment + gen:
  private final Path segsRootPath;

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

  public ReindexingReader(Directory indexDir, Path segsRootPath) throws IOException {

    this.indexDir = indexDir;

    // Per-segment parallel indices are stored under subdirs "segs":
    this.segsRootPath = segsRootPath;
    Files.createDirectories(segsRootPath);

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
    mgr = new ReaderManager(new ParallelLeafDirectoryReader(DirectoryReader.open(w, true)));
    if (DEBUG) {
      DirectoryReader r = mgr.acquire();
      try {
        System.out.println("TEST: ReindexingReader.init current=" + r);
      } finally {
        mgr.release(r);
      }
    }
  }

  protected abstract IndexWriterConfig getIndexWriterConfig() throws IOException;

  /** Optional method to validate that the provided parallel reader in fact reflects the changes in schemaGen. */
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
      assert parallelReader instanceof ParallelLeafReader == false;
      assert reader instanceof ParallelLeafReader == false;

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
    private final FieldTypes fieldTypes;
    public ParallelLeafDirectoryReader(DirectoryReader in) {
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

      // TODO: move this logic up?
      fieldTypes = new FieldTypes(in.getFieldTypes());
      for(LeafReaderContext ctx : leaves()) {
        LeafReader leafReader = ctx.reader();
        if (leafReader instanceof ParallelLeafReader) {
          fieldTypes.addAll(((ParallelLeafReader) leafReader).getParallelReaders()[0].getFieldTypes());
        }
      }
    }
      
    @Override
    public FieldTypes getFieldTypes() {
      return fieldTypes;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
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
    if (DEBUG) System.out.println("TEST: after close writer index=" + SegmentInfos.readLatestCommit(indexDir).toString(indexDir));

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
      assert liveIDs.contains(segIDGen.segID);
    }

    boolean fail = false;
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(segsRootPath)) {
        for (Path path : stream) {
          SegmentIDAndGen segIDGen = new SegmentIDAndGen(path.getFileName().toString());
          if (liveIDs.contains(segIDGen.segID) == false) {
            if (DEBUG) System.out.println("TEST: fail seg=" + path.getFileName() + " is not live but still has a parallel index");
            fail = true;
          }
        }
      }
    assert fail == false;
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

          Path leafIndex = segsRootPath.resolve(segIDGen.toString());

          final Directory dir = openDirectory(leafIndex);

          if (Files.exists(leafIndex.resolve("done")) == false) {
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
          final LeafReader parLeafReader;
          if (infos.size() == 1) {
            parLeafReader = new SegmentReader(FieldTypes.getFieldTypes(infos, null, null), infos.info(0), IOContext.DEFAULT);
          } else {
            // This just means we didn't forceMerge above:
            parLeafReader = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir));
          }

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
    if (DEBUG) System.out.println("TEST: prune; removeOldGens=" + removeOldGens);

    Set<String> liveIDs = new HashSet<String>();
    for(SegmentCommitInfo info : lastCommit) {
      String idString = StringHelper.idToString(info.info.getId());
      liveIDs.add(idString);
      if (DEBUG) System.out.println("TEST: live id " + idString + " seg=" + info);
    }

    long currentSchemaGen = getCurrentSchemaGen();

    if (Files.exists(segsRootPath)) {
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(segsRootPath)) {
          for (Path path : stream) {
            if (Files.isDirectory(path)) {
              SegmentIDAndGen segIDGen = new SegmentIDAndGen(path.getFileName().toString());
              assert segIDGen.schemaGen <= currentSchemaGen;
              if (DEBUG) System.out.println("TEST: check dir=" + path + " live?=" + (liveIDs.contains(segIDGen.segID)) + " closed=" + (closedSegments.contains(segIDGen)) + " currentSchemaGen=" + currentSchemaGen);
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
  }

  /** Just replaces the sub-readers with parallel readers, so reindexed fields are merged into new segments. */
  private class ReindexingMergePolicy extends MergePolicy {

    class ReindexingOneMerge extends OneMerge {

      List<LeafReader> parallelReaders;
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
      public List<LeafReader> getMergeReaders() throws IOException {
        if (parallelReaders == null) {
          parallelReaders = new ArrayList<>();
          for (LeafReader reader : super.getMergeReaders()) {
            parallelReaders.add(getCurrentReader(reader, schemaGen));
          }
        }

        return parallelReaders;
      }

      @Override
      public void mergeFinished() throws IOException {
        Throwable th = null;
        for(LeafReader r : parallelReaders) {
          if (r instanceof ParallelLeafReader) {
            try {
              r.decRef();
            } catch (Throwable t) {
              if (th == null) {
                th = t;
              }
            }
          }
        }

        // If any error occured, throw it.
        IOUtils.reThrow(th);
      }
    
      @Override
      public void setInfo(SegmentCommitInfo info) {
        // Record that this merged segment is current as of this schemaGen:
        info.info.getDiagnostics().put(SCHEMA_GEN_KEY, Long.toString(schemaGen));
        super.setInfo(info);
      }

      @Override
      public MergePolicy.DocMap getDocMap(final MergeState mergeState) {
        return super.getDocMap(mergeState);
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

    final MergePolicy in;

    /** Create a new {@code MergePolicy} that sorts documents with the given {@code sort}. */
    public ReindexingMergePolicy(MergePolicy in) {
      this.in = in;
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

