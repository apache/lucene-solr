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
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.store.Directory;

/**
 * nocommit - javadoc
 * @lucene.experimental
 */
public class PerDocWriteState {
  public final PrintStream infoStream;
  public final Directory directory;
  public final String segmentName;
  public final FieldInfos fieldInfos;
  public final AtomicLong bytesUsed;
  public final SegmentCodecs segmentCodecs;
  public final int codecId;

  /** Expert: The fraction of terms in the "dictionary" which should be stored
   * in RAM.  Smaller values use more memory, but make searching slightly
   * faster, while larger values use less memory and make searching slightly
   * slower.  Searching is typically not dominated by dictionary lookup, so
   * tweaking this is rarely useful.*/
  public int termIndexInterval;                   // TODO: this should be private to the codec, not settable here or in IWC

  public PerDocWriteState(PrintStream infoStream, Directory directory, String segmentName, FieldInfos fieldInfos, AtomicLong bytesUsed, int codecId) {
    this.infoStream = infoStream;
    this.directory = directory;
    this.segmentName = segmentName;
    this.fieldInfos = fieldInfos;
    this.segmentCodecs = fieldInfos.buildSegmentCodecs(false);
    this.codecId = codecId;
    this.bytesUsed = bytesUsed;
  }
  
  public PerDocWriteState(SegmentWriteState state) {
    infoStream = state.infoStream;
    directory = state.directory;
    segmentCodecs = state.segmentCodecs;
    segmentName = state.segmentName;
    fieldInfos = state.fieldInfos;
    codecId = state.codecId;
    bytesUsed = new AtomicLong(0);
  }
  
  public PerDocWriteState(PerDocWriteState state, int codecId) {
    this.infoStream = state.infoStream;
    this.directory = state.directory;
    this.segmentName = state.segmentName;
    this.fieldInfos = state.fieldInfos;
    this.segmentCodecs = state.segmentCodecs;
    this.codecId = codecId;
    this.bytesUsed = state.bytesUsed;
  }
  
  
  public String codecIdAsString() {
    return "" + codecId;
  }
}
