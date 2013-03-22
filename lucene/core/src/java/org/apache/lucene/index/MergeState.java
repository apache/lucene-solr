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
import org.apache.lucene.util.packed.MonotonicAppendingLongBuffer;

/** Holds common state used during segment merging.
 *
 * @lucene.experimental */
public class MergeState {

  /**
   * Remaps docids around deletes during merge
   */
  public static abstract class DocMap {

    DocMap() {}

    /** Returns the mapped docID corresponding to the provided one. */
    public abstract int get(int docID);

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

    /** Creates a {@link DocMap} instance appropriate for
     *  this reader. */
    public static DocMap build(AtomicReader reader) {
      final int maxDoc = reader.maxDoc();
      if (!reader.hasDeletions()) {
        return new NoDelDocMap(maxDoc);
      }
      final Bits liveDocs = reader.getLiveDocs();
      return build(maxDoc, liveDocs);
    }

    static DocMap build(final int maxDoc, final Bits liveDocs) {
      assert liveDocs != null;
      final MonotonicAppendingLongBuffer docMap = new MonotonicAppendingLongBuffer();
      int del = 0;
      for (int i = 0; i < maxDoc; ++i) {
        docMap.add(i - del);
        if (!liveDocs.get(i)) {
          ++del;
        }
      }
      final int numDeletedDocs = del;
      assert docMap.size() == maxDoc;
      return new DocMap() {

        @Override
        public int get(int docID) {
          if (!liveDocs.get(docID)) {
            return -1;
          }
          return (int) docMap.get(docID);
        }

        @Override
        public int maxDoc() {
          return maxDoc;
        }

        @Override
        public int numDeletedDocs() {
          return numDeletedDocs;
        }

      };
    }

  }

  private static class NoDelDocMap extends DocMap {

    private final int maxDoc;

    private NoDelDocMap(int maxDoc) {
      this.maxDoc = maxDoc;
    }

    @Override
    public int get(int docID) {
      return docID;
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

  /** {@link SegmentInfo} of the newly merged segment. */
  public final SegmentInfo segmentInfo;

  /** {@link FieldInfos} of the newly merged segment. */
  public FieldInfos fieldInfos;

  /** Readers being merged. */
  public final List<AtomicReader> readers;

  /** Maps docIDs around deletions. */
  public DocMap[] docMaps;

  /** New docID base per reader. */
  public int[] docBase;

  /** Holds the CheckAbort instance, which is invoked
   *  periodically to see if the merge has been aborted. */
  public final CheckAbort checkAbort;

  /** InfoStream for debugging messages. */
  public final InfoStream infoStream;

  // TODO: get rid of this? it tells you which segments are 'aligned' (e.g. for bulk merging)
  // but is this really so expensive to compute again in different components, versus once in SM?

  /** {@link SegmentReader}s that have identical field
   * name/number mapping, so their stored fields and term
   * vectors may be bulk merged. */
  public SegmentReader[] matchingSegmentReaders;

  /** How many {@link #matchingSegmentReaders} are set. */
  public int matchedCount;

  /** Sole constructor. */
  MergeState(List<AtomicReader> readers, SegmentInfo segmentInfo, InfoStream infoStream, CheckAbort checkAbort) {
    this.readers = readers;
    this.segmentInfo = segmentInfo;
    this.infoStream = infoStream;
    this.checkAbort = checkAbort;
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
