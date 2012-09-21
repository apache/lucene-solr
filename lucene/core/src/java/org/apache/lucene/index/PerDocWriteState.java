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

import org.apache.lucene.codecs.PerDocConsumer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.InfoStream;

/**
 * Encapsulates all necessary state to initiate a {@link PerDocConsumer} and
 * create all necessary files in order to consume and merge per-document values.
 * 
 * @lucene.experimental
 */
public class PerDocWriteState {
  /** InfoStream used for debugging. */
  public final InfoStream infoStream;

  /** {@link Directory} to write all files to. */
  public final Directory directory;

  /** {@link SegmentInfo} describing this segment. */
  public final SegmentInfo segmentInfo;

  /** Number of bytes allocated in RAM to hold this state. */
  public final Counter bytesUsed;

  /** Segment suffix to pass to {@link
   * IndexFileNames#segmentFileName(String,String,String)}. */
  public final String segmentSuffix;

  /** {@link IOContext} to use for all file writing. */
  public final IOContext context;

  /** Creates a {@code PerDocWriteState}. */
  public PerDocWriteState(InfoStream infoStream, Directory directory,
      SegmentInfo segmentInfo, Counter bytesUsed,
      String segmentSuffix, IOContext context) {
    this.infoStream = infoStream;
    this.directory = directory;
    this.segmentInfo = segmentInfo;
    this.segmentSuffix = segmentSuffix;
    this.bytesUsed = bytesUsed;
    this.context = context;
  }

  /** Creates a {@code PerDocWriteState}, copying fields
   *  from another and allocating a new {@link #bytesUsed}. */
  public PerDocWriteState(SegmentWriteState state) {
    infoStream = state.infoStream;
    directory = state.directory;
    segmentInfo = state.segmentInfo;
    segmentSuffix = state.segmentSuffix;
    bytesUsed = Counter.newCounter();
    context = state.context;
  }

  /** Creates a {@code PerDocWriteState}, copying fields
   *  from another (copy constructor) but setting a new
   *  {@link #segmentSuffix}. */
  public PerDocWriteState(PerDocWriteState state, String segmentSuffix) {
    this.infoStream = state.infoStream;
    this.directory = state.directory;
    this.segmentInfo = state.segmentInfo;
    this.segmentSuffix = segmentSuffix;
    this.bytesUsed = state.bytesUsed;
    this.context = state.context;
  }
}
