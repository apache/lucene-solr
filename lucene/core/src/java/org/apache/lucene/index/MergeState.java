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

import java.util.List;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.packed.PackedInts;

/** Holds common state used during segment merging.
 *
 * @lucene.experimental */
public class MergeState {

  /**
   * Remaps docids around deletes during merge
   */
  public static abstract class DocMap {
    private final Bits liveDocs;

    /** Sole constructor. (For invocation by subclass 
     *  constructors, typically implicit.) */
    protected DocMap(Bits liveDocs) {
      this.liveDocs = liveDocs;
    }

    /** Creates a {@link DocMap} instance appropriate for
     *  this reader. */
    public static DocMap build(AtomicReader reader) {
      final int maxDoc = reader.maxDoc();
      final int numDeletes = reader.numDeletedDocs();
      final int numDocs = maxDoc - numDeletes;
      assert reader.getLiveDocs() != null || numDeletes == 0;
      if (numDeletes == 0) {
        return new NoDelDocMap(maxDoc);
      } else if (numDeletes < numDocs) {
        return buildDelCountDocmap(maxDoc, numDeletes, reader.getLiveDocs(), PackedInts.COMPACT);
      } else {
        return buildDirectDocMap(maxDoc, numDocs, reader.getLiveDocs(), PackedInts.COMPACT);
      }
    }

    static DocMap buildDelCountDocmap(int maxDoc, int numDeletes, Bits liveDocs, float acceptableOverheadRatio) {
      PackedInts.Mutable numDeletesSoFar = PackedInts.getMutable(maxDoc,
          PackedInts.bitsRequired(numDeletes), acceptableOverheadRatio);
      int del = 0;
      for (int i = 0; i < maxDoc; ++i) {
        if (!liveDocs.get(i)) {
          ++del;
        }
        numDeletesSoFar.set(i, del);
      }
      assert del == numDeletes : "del=" + del + ", numdeletes=" + numDeletes;
      return new DelCountDocMap(liveDocs, numDeletesSoFar);
    }

    static DocMap buildDirectDocMap(int maxDoc, int numDocs, Bits liveDocs, float acceptableOverheadRatio) {
      PackedInts.Mutable docIds = PackedInts.getMutable(maxDoc,
          PackedInts.bitsRequired(Math.max(0, numDocs - 1)), acceptableOverheadRatio);
      int del = 0;
      for (int i = 0; i < maxDoc; ++i) {
        if (liveDocs.get(i)) {
          docIds.set(i, i - del);
        } else {
          ++del;
        }
      }
      assert numDocs + del == maxDoc : "maxDoc=" + maxDoc + ", del=" + del + ", numDocs=" + numDocs;
      return new DirectDocMap(liveDocs, docIds, del);
    }

    /** Returns the mapped docID corresponding to the provided one. */
    public int get(int docId) {
      if (liveDocs == null || liveDocs.get(docId)) {
        return remap(docId);
      } else {
        return -1;
      }
    }

    /** Returns the mapped docID corresponding to the provided one. */
    public abstract int remap(int docId);

    /** Returns the total number of documents, ignoring
     *  deletions. */
    public abstract int maxDoc();

    /** Returns the number of not-deleted documents. */
    public final int numDocs() {
      return maxDoc() - numDeletedDocs();
    }

    /** Returns the number of deleted documents. */
    public abstract int numDeletedDocs();

    /** Returns true if there are any deletions. */
    public boolean hasDeletions() {
      return numDeletedDocs() > 0;
    }

  }

  private static class NoDelDocMap extends DocMap {

    private final int maxDoc;

    private NoDelDocMap(int maxDoc) {
      super(null);
      this.maxDoc = maxDoc;
    }

    @Override
    public int remap(int docId) {
      return docId;
    }

    @Override
    public int maxDoc() {
      return maxDoc;
    }

    @Override
    public int numDeletedDocs() {
      return 0;
    }
  }

  private static class DirectDocMap extends DocMap {

    private final PackedInts.Mutable docIds;
    private final int numDeletedDocs;

    private DirectDocMap(Bits liveDocs, PackedInts.Mutable docIds, int numDeletedDocs) {
      super(liveDocs);
      this.docIds = docIds;
      this.numDeletedDocs = numDeletedDocs;
    }

    @Override
    public int remap(int docId) {
      return (int) docIds.get(docId);
    }

    @Override
    public int maxDoc() {
      return docIds.size();
    }

    @Override
    public int numDeletedDocs() {
      return numDeletedDocs;
    }
  }

  private static class DelCountDocMap extends DocMap {

    private final PackedInts.Mutable numDeletesSoFar;

    private DelCountDocMap(Bits liveDocs, PackedInts.Mutable numDeletesSoFar) {
      super(liveDocs);
      this.numDeletesSoFar = numDeletesSoFar;
    }

    @Override
    public int remap(int docId) {
      return docId - (int) numDeletesSoFar.get(docId);
    }

    @Override
    public int maxDoc() {
      return numDeletesSoFar.size();
    }

    @Override
    public int numDeletedDocs() {
      final int maxDoc = maxDoc();
      return (int) numDeletesSoFar.get(maxDoc - 1);
    }
  }

  /** {@link SegmentInfo} of the newly merged segment. */
  public SegmentInfo segmentInfo;

  /** {@link FieldInfos} of the newly merged segment. */
  public FieldInfos fieldInfos;

  /** Readers being merged. */
  public List<AtomicReader> readers;

  /** Maps docIDs around deletions. */
  public DocMap[] docMaps;

  /** New docID base per reader. */
  public int[] docBase;

  /** Holds the CheckAbort instance, which is invoked
   *  periodically to see if the merge has been aborted. */
  public CheckAbort checkAbort;
  
  /** InfoStream for debugging messages. */
  public InfoStream infoStream;

  /** Current field being merged. */
  public FieldInfo fieldInfo;
  
  // TODO: get rid of this? it tells you which segments are 'aligned' (e.g. for bulk merging)
  // but is this really so expensive to compute again in different components, versus once in SM?
  
  /** {@link SegmentReader}s that have identical field
   * name/number mapping, so their stored fields and term
   * vectors may be bulk merged. */
  public SegmentReader[] matchingSegmentReaders;

  /** How many {@link #matchingSegmentReaders} are set. */
  public int matchedCount;

  /** Sole constructor. */
  MergeState() {
  }
  
  /**
   * Class for recording units of work when merging segments.
   */
  public static class CheckAbort {
    private double workCount;
    private final MergePolicy.OneMerge merge;
    private final Directory dir;

    /** Creates a #CheckAbort instance. */
    public CheckAbort(MergePolicy.OneMerge merge, Directory dir) {
      this.merge = merge;
      this.dir = dir;
    }

    /**
     * Records the fact that roughly units amount of work
     * have been done since this method was last called.
     * When adding time-consuming code into SegmentMerger,
     * you should test different values for units to ensure
     * that the time in between calls to merge.checkAborted
     * is up to ~ 1 second.
     */
    public void work(double units) throws MergePolicy.MergeAbortedException {
      workCount += units;
      if (workCount >= 10000.0) {
        merge.checkAborted(dir);
        workCount = 0;
      }
    }
    
    /** If you use this: IW.close(false) cannot abort your merge!
     * @lucene.internal */
    static final MergeState.CheckAbort NONE = new MergeState.CheckAbort(null, null) {
      @Override
      public void work(double units) {
        // do nothing
      }
    };
  }
}
