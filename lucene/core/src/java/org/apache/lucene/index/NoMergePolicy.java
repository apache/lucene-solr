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
import java.util.Map;

/**
 * A {@link MergePolicy} which never returns merges to execute (hence it's
 * name). It is also a singleton and can be accessed through
 * {@link NoMergePolicy#NO_COMPOUND_FILES} if you want to indicate the index
 * does not use compound files, or through {@link NoMergePolicy#COMPOUND_FILES}
 * otherwise. Use it if you want to prevent an {@link IndexWriter} from ever
 * executing merges, without going through the hassle of tweaking a merge
 * policy's settings to achieve that, such as changing its merge factor.
 */
public final class NoMergePolicy extends MergePolicy {

  /**
   * A singleton {@link NoMergePolicy} which indicates the index does not use
   * compound files.
   */
  public static final MergePolicy NO_COMPOUND_FILES = new NoMergePolicy(false);

  /**
   * A singleton {@link NoMergePolicy} which indicates the index uses compound
   * files.
   */
  public static final MergePolicy COMPOUND_FILES = new NoMergePolicy(true);

  private final boolean useCompoundFile;
  
  private NoMergePolicy(boolean useCompoundFile) {
    // prevent instantiation
    this.useCompoundFile = useCompoundFile;
  }

  @Override
  public void close() {}

  @Override
  public MergeSpecification findMerges(SegmentInfos segmentInfos)
      throws CorruptIndexException, IOException { return null; }

  @Override
  public MergeSpecification findForcedMerges(SegmentInfos segmentInfos,
             int maxSegmentCount, Map<SegmentInfo,Boolean> segmentsToMerge)
      throws CorruptIndexException, IOException { return null; }

  @Override
  public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos)
      throws CorruptIndexException, IOException { return null; }

  @Override
  public boolean useCompoundFile(SegmentInfos segments, SegmentInfo newSegment) { return useCompoundFile; }

  @Override
  public void setIndexWriter(IndexWriter writer) {}

  @Override
  public String toString() {
    return "NoMergePolicy";
  }
}
