package org.apache.lucene.index;

/**
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

import org.apache.lucene.index.PayloadProcessorProvider.ReaderPayloadProcessor;
import org.apache.lucene.index.PayloadProcessorProvider.PayloadProcessor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.InfoStream;

/** Holds common state used during segment merging
 *
 * @lucene.experimental */
public class MergeState {

  public static class IndexReaderAndLiveDocs {
    public final AtomicReader reader;
    public final Bits liveDocs;

    public IndexReaderAndLiveDocs(AtomicReader reader, Bits liveDocs) {
      this.reader = reader;
      this.liveDocs = liveDocs;
    }
  }

  public FieldInfos fieldInfos;
  public List<IndexReaderAndLiveDocs> readers;        // Readers & liveDocs being merged
  public int[][] docMaps;                             // Maps docIDs around deletions
  public int[] docBase;                               // New docID base per reader
  public int mergedDocCount;                          // Total # merged docs
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
    private MergePolicy.OneMerge merge;
    private Directory dir;
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
      public void work(double units) throws MergePolicy.MergeAbortedException {
        // do nothing
      }
    };
  }
}
