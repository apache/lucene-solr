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

import org.apache.lucene.codecs.PostingsFormat; // javadocs
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat; // javadocs
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.MutableBits;

/**
 * Holder class for common parameters used during write.
 * @lucene.experimental
 */
public class SegmentWriteState {

  /** {@link InfoStream} used for debugging messages. */
  public final InfoStream infoStream;

  /** {@link Directory} where this segment will be written
   *  to. */
  public final Directory directory;

  /** {@link SegmentInfo} describing this segment. */
  public final SegmentInfo segmentInfo;

  /** {@link FieldInfos} describing all fields in this
   *  segment. */
  public final FieldInfos fieldInfos;

  /** Number of deleted documents set while flushing the
   *  segment. */
  public int delCountOnFlush;

  /** Deletes to apply while we are flushing the segment.  A
   *  Term is enrolled in here if it was deleted at one
   *  point, and it's mapped to the docIDUpto, meaning any
   *  docID &lt; docIDUpto containing this term should be
   *  deleted. */
  public final BufferedDeletes segDeletes;

  /** {@link MutableBits} recording live documents; this is
   *  only set if there is one or more deleted documents. */
  public MutableBits liveDocs;

  /** Unique suffix for any postings files written for this
   *  segment.  {@link PerFieldPostingsFormat} sets this for
   *  each of the postings formats it wraps.  If you create
   *  a new {@link PostingsFormat} then any files you
   *  write/read must be derived using this suffix (use
   *  {@link IndexFileNames#segmentFileName(String,String,String)}). */
  public final String segmentSuffix;

  /** Expert: The fraction of terms in the "dictionary" which should be stored
   * in RAM.  Smaller values use more memory, but make searching slightly
   * faster, while larger values use less memory and make searching slightly
   * slower.  Searching is typically not dominated by dictionary lookup, so
   * tweaking this is rarely useful.*/
  public int termIndexInterval;                   // TODO: this should be private to the codec, not settable here or in IWC
  
  /** {@link IOContext} for all writes; you should pass this
   *  to {@link Directory#createOutput(String,IOContext)}. */
  public final IOContext context;

  /** Sole constructor. */
  public SegmentWriteState(InfoStream infoStream, Directory directory, SegmentInfo segmentInfo, FieldInfos fieldInfos,
      int termIndexInterval, BufferedDeletes segDeletes, IOContext context) {
    this.infoStream = infoStream;
    this.segDeletes = segDeletes;
    this.directory = directory;
    this.segmentInfo = segmentInfo;
    this.fieldInfos = fieldInfos;
    this.termIndexInterval = termIndexInterval;
    segmentSuffix = "";
    this.context = context;
  }
  
  /**
   * Create a shallow {@link SegmentWriteState} copy final a format ID
   */
  public SegmentWriteState(SegmentWriteState state, String segmentSuffix) {
    infoStream = state.infoStream;
    directory = state.directory;
    segmentInfo = state.segmentInfo;
    fieldInfos = state.fieldInfos;
    termIndexInterval = state.termIndexInterval;
    context = state.context;
    this.segmentSuffix = segmentSuffix;
    segDeletes = state.segDeletes;
    delCountOnFlush = state.delCountOnFlush;
  }
}
