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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.lucene.store.Directory;

/** Embeds a [read-only] SegmentInfo and adds per-commit
 *  fields.
 *
 *  @lucene.experimental */

// nocommit this class feels alot like ReaderAndLiveDocs...?
// like it carries mutable per-segment state....?
public class SegmentInfoPerCommit {

  public final SegmentInfo info;

  // How many deleted docs in the segment:
  private int delCount;

  // Generation number of the live docs file (-1 if there
  // are no deletes yet):
  private long delGen;

  public SegmentInfoPerCommit(SegmentInfo info, int delCount, long delGen) {
    this.info = info;
    this.delCount = delCount;
    this.delGen = delGen;
  }

  void advanceDelGen() {
    if (delGen == -1) {
      delGen = 1;
    } else {
      delGen++;
    }
    info.clearSizeInBytes();
  }

  public long sizeInBytes() throws IOException {
    // nocommit add in live docs size
    return info.sizeInBytes();
  }

  public Collection<String> files() throws IOException {
    Collection<String> files = new HashSet<String>(info.files());

    // nocommit make this take list instead...?
    // Must separately add any live docs files:
    info.getCodec().liveDocsFormat().files(this, files);

    return files;
  }

  // NOTE: only used in-RAM by IW to track buffered deletes;
  // this is never written to/read from the Directory
  private long bufferedDeletesGen;
  
  long getBufferedDeletesGen() {
    return bufferedDeletesGen;
  }

  void setBufferedDeletesGen(long v) {
    bufferedDeletesGen = v;
  }
  
  void clearDelGen() {
    delGen = -1;
    info.clearSizeInBytes();
  }

  public void setDelGen(long delGen) {
    this.delGen = delGen;
    info.clearSizeInBytes();
  }

  public boolean hasDeletions() {
    return delGen != -1;
  }

  public long getNextDelGen() {
    if (delGen == -1) {
      return 1;
    } else {
      return delGen + 1;
    }
  }

  public long getDelGen() {
    return delGen;
  }
  
  public int getDelCount() {
    return delCount;
  }

  void setDelCount(int delCount) {
    this.delCount = delCount;
    assert delCount <= info.docCount;
  }

  public String toString(Directory dir, int pendingDelCount) {
    return info.toString(dir, delCount + pendingDelCount);
  }

  @Override
  public SegmentInfoPerCommit clone() {
    // nocommit ok?  SI is immutable!?
    return new SegmentInfoPerCommit(info, delCount, delGen);
  }
}
