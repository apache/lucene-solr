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

import org.apache.lucene.index.PayloadProcessorProvider.PayloadProcessor;
import org.apache.lucene.index.PayloadProcessorProvider.ReaderPayloadProcessor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.packed.PackedInts;

/** Holds common state used during segment merging
 *
 * @lucene.experimental */
public class MergeState {

  public static class IndexReaderAndLiveDocs {
    public final AtomicReader reader;
    public final Bits liveDocs;
    public final int numDeletedDocs;

    public IndexReaderAndLiveDocs(AtomicReader reader, Bits liveDocs, int numDeletedDocs) {
      this.reader = reader;
      this.liveDocs = liveDocs;
      this.numDeletedDocs = numDeletedDocs;
    }
  }

  public static abstract class DocMap {
    private final Bits liveDocs;

    protected DocMap(Bits liveDocs) {
      this.liveDocs = liveDocs;
    }

    public static DocMap build(IndexReaderAndLiveDocs reader) {
      final int maxDoc = reader.reader.maxDoc();
      final int numDeletes = reader.numDeletedDocs;
      final int numDocs = maxDoc - numDeletes;
      assert reader.liveDocs != null || numDeletes == 0;
      if (numDeletes == 0) {
        return new NoDelDocMap(maxDoc);
      } else if (numDeletes < numDocs) {
        return buildDelCountDocmap(maxDoc, numDeletes, reader.liveDocs, PackedInts.COMPACT);
      } else {
        return buildDirectDocMap(maxDoc, numDocs, reader.liveDocs, PackedInts.COMPACT);
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

    public int get(int docId) {
      if (liveDocs == null || liveDocs.get(docId)) {
        return remap(docId);
      } else {
        return -1;
      }
    }

    public abstract int remap(int docId);

    public abstract int maxDoc();

    public final int numDocs() {
      return maxDoc() - numDeletedDocs();
    }

    public abstract int numDeletedDocs();

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

  public SegmentInfo segmentInfo;
  public FieldInfos fieldInfos;
  public List<IndexReaderAndLiveDocs> readers;        // Readers & liveDocs being merged
  public DocMap[] docMaps;                            // Maps docIDs around deletions
  public int[] docBase;                               // New docID base per reader
  public CheckAbort checkAbort;
  public InfoStream infoStream;

  // Updated per field;
  public FieldInfo fieldInfo;
  
  // Used to process payloads
  // TODO: this is a FactoryFactory here basically
  // and we could make a codec(wrapper) to do all of this privately so IW is uninvolved
  public PayloadProcessorProvider payloadProcessorProvider;
  public ReaderPayloadProcessor[] readerPayloadProcessor;
  public PayloadProcessor[] currentPayloadProcessor;

  // TODO: get rid of this? it tells you which segments are 'aligned' (e.g. for bulk merging)
  // but is this really so expensive to compute again in different components, versus once in SM?
  public SegmentReader[] matchingSegmentReaders;
  public int matchedCount;
  
  public static class CheckAbort {
    private double workCount;
    private final MergePolicy.OneMerge merge;
    private final Directory dir;
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
