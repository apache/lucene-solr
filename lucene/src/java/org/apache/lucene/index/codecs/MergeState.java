package org.apache.lucene.index.codecs;

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

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.PayloadProcessorProvider.DirPayloadProcessor;
import org.apache.lucene.index.PayloadProcessorProvider.PayloadProcessor;
import org.apache.lucene.store.Directory;

/** Holds common state used during segment merging
 *
 * @lucene.experimental */
public class MergeState {
  public FieldInfos fieldInfos;
  public List<IndexReader> readers;               // Readers being merged
  public int readerCount;                         // Number of readers being merged
  public int[][] docMaps;                         // Maps docIDs around deletions
  public int[] docBase;                           // New docID base per reader
  public int mergedDocCount;                      // Total # merged docs
  public CheckAbort checkAbort;

  // Updated per field;
  public FieldInfo fieldInfo;
  
  // Used to process payloads
  public boolean hasPayloadProcessorProvider;
  public DirPayloadProcessor[] dirPayloadProcessor;
  public PayloadProcessor[] currentPayloadProcessor;

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
  }
}
