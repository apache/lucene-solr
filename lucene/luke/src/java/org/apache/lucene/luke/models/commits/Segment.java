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

package org.apache.lucene.luke.models.commits;

import java.io.IOException;

import org.apache.lucene.index.SegmentCommitInfo;

/**
 * Holder for a segment.
 */
public final class Segment {

  private String name;

  private int maxDoc;

  private long delGen;

  private int delCount;

  private String luceneVer;

  private String codecName;

  private String displaySize;

  private boolean useCompoundFile;

  static Segment of(SegmentCommitInfo segInfo) {
    Segment segment = new Segment();
    segment.name = segInfo.info.name;
    segment.maxDoc = segInfo.info.maxDoc();
    segment.delGen = segInfo.getDelGen();
    segment.delCount = segInfo.getDelCount();
    segment.luceneVer = segInfo.info.getVersion().toString();
    segment.codecName = segInfo.info.getCodec().getName();
    try {
      segment.displaySize = CommitsImpl.toDisplaySize(segInfo.sizeInBytes());
    } catch (IOException e) {
    }
    segment.useCompoundFile = segInfo.info.getUseCompoundFile();
    return segment;
  }

  public String getName() {
    return name;
  }

  public int getMaxDoc() {
    return maxDoc;
  }

  public long getDelGen() {
    return delGen;
  }

  public int getDelCount() {
    return delCount;
  }

  public String getLuceneVer() {
    return luceneVer;
  }

  public String getCodecName() {
    return codecName;
  }

  public String getDisplaySize() {
    return displaySize;
  }

  public boolean isUseCompoundFile() {
    return useCompoundFile;
  }

  private Segment() {
  }
}
