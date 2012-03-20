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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.MutableBits;

/**
 * Holder class for common parameters used during write.
 * @lucene.experimental
 */
public class SegmentWriteState {
  public final InfoStream infoStream;
  public final Directory directory;
  public final String segmentName;
  public final FieldInfos fieldInfos;
  public final int numDocs;
  public int delCountOnFlush;

  // Deletes to apply while we are flushing the segment.  A
  // Term is enrolled in here if it was deleted at one
  // point, and it's mapped to the docIDUpto, meaning any
  // docID < docIDUpto containing this term should be
  // deleted.
  public final BufferedDeletes segDeletes;

  // Lazily created:
  public MutableBits liveDocs;

  public final Codec codec;
  public final String segmentSuffix;

  /** Expert: The fraction of terms in the "dictionary" which should be stored
   * in RAM.  Smaller values use more memory, but make searching slightly
   * faster, while larger values use less memory and make searching slightly
   * slower.  Searching is typically not dominated by dictionary lookup, so
   * tweaking this is rarely useful.*/
  public int termIndexInterval;                   // TODO: this should be private to the codec, not settable here or in IWC
  
  public final IOContext context;

  public SegmentWriteState(InfoStream infoStream, Directory directory, String segmentName, FieldInfos fieldInfos,
      int numDocs, int termIndexInterval, Codec codec, BufferedDeletes segDeletes, IOContext context) {
    this.infoStream = infoStream;
    this.segDeletes = segDeletes;
    this.directory = directory;
    this.segmentName = segmentName;
    this.fieldInfos = fieldInfos;
    this.numDocs = numDocs;
    this.termIndexInterval = termIndexInterval;
    this.codec = codec;
    segmentSuffix = "";
    this.context = context;
  }
  
  /**
   * Create a shallow {@link SegmentWriteState} copy final a format ID
   */
  public SegmentWriteState(SegmentWriteState state, String segmentSuffix) {
    infoStream = state.infoStream;
    directory = state.directory;
    segmentName = state.segmentName;
    fieldInfos = state.fieldInfos;
    numDocs = state.numDocs;
    termIndexInterval = state.termIndexInterval;
    context = state.context;
    codec = state.codec;
    this.segmentSuffix = segmentSuffix;
    segDeletes = state.segDeletes;
    delCountOnFlush = state.delCountOnFlush;
  }
}
