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

package org.apache.solr.util;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.LuceneTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link MergePolicy} with a no-arg constructor that proxies to an
 * instance retrieved from {@link LuceneTestCase#newMergePolicy}.
 * Solr tests utilizing the Lucene randomized test framework can refer 
 * to this class in solrconfig.xml to get a fully randomized merge policy.
 */
public class RandomMergePolicy extends MergePolicy {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  /** 
   * Not private so tests can inspect it, 
   */
  final MergePolicy inner;

  public RandomMergePolicy() {
    this(LuceneTestCase.newMergePolicy());
  }

  protected RandomMergePolicy(MergePolicy inner) {
    super(inner.getNoCFSRatio(), 
          (long) (inner.getMaxCFSSegmentSizeMB() * 1024 * 1024));
    this.inner = inner;
    log.info("RandomMergePolicy wrapping {}: {}",
             inner.getClass(), inner);
  }

  @Override
  public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, IndexWriter writer) 
    throws IOException {

    return inner.findForcedDeletesMerges(segmentInfos, writer);
  }

  @Override
  public MergeSpecification findForcedMerges(SegmentInfos segmentInfos, 
                                             int maxSegmentCount, 
                                             Map<SegmentCommitInfo,Boolean> segmentsToMerge,
                                             IndexWriter writer) 
    throws IOException {
    
    return inner.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, writer);
  }

  @Override
  public MergeSpecification findMerges(MergeTrigger mergeTrigger, 
                                       SegmentInfos segmentInfos,
                                       IndexWriter writer)
    throws IOException {

    return inner.findMerges(mergeTrigger, segmentInfos, writer);
  }

  @Override
  public boolean useCompoundFile(SegmentInfos infos,
                                 SegmentCommitInfo mergedInfo,
                                 IndexWriter writer)
    throws IOException {
    
    return inner.useCompoundFile(infos, mergedInfo, writer);
  }

}
