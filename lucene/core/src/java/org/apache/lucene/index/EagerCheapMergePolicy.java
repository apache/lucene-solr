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

package org.apache.lucene.index;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.lucene.util.InfoStream;

/**
 * Eagerly proposes cheap merges on a commit. The goal is to help keep the segment count low where
 * we can do so cheaply, particularly on a commit so that the IndexSearcher has fewer segments to
 * search over.
 */
public class EagerCheapMergePolicy extends TieredMergePolicy {
  // Extends TieredMergePolicy for convenience but we could adjust to delegate or modify TieredMergePolicy

  private static final String INFO_STREAM_CATEGORY = "CHEAP_MP";

  private long cheapMergeThresholdBytes = 2 * 1024 * 1024; // 2MB
  private int cheapMinMergeAtOnce = 3;
  private double cheapMaxSizeRatio = 1.0;
  private int cheapLimitConcurrentMergeSegments = 0; // thus only when no merges in-progress

  /** Sole constructor. */
  public EagerCheapMergePolicy() {
  }

  /** Returns {@link #setCheapMergeThresholdMB(double)}. */
  public long getCheapMergeThresholdBytes() {
    return cheapMergeThresholdBytes;
  }

  /** Limit cheap merges to those less than this size in megabytes. */
  public EagerCheapMergePolicy setCheapMergeThresholdMB(double v) {
    v *= 1024 * 1024;
    cheapMergeThresholdBytes = (v > Long.MAX_VALUE) ? Long.MAX_VALUE : (long) v;
    return this;
  }

  /** Returns {@link #setCheapMinMergeAtOnce(int)}. */
  public int getCheapMinMergeAtOnce() {
    return cheapMinMergeAtOnce;
  }

  /** Must have at least this many cheap segments to suggest merging them. */
  public EagerCheapMergePolicy setCheapMinMergeAtOnce(int cheapMinMergeAtOnce) {
    this.cheapMinMergeAtOnce = cheapMinMergeAtOnce;
    return this;
  }

  /** Returns {@link #setCheapMaxSizeRatio(double)}. */
  public double getCheapMaxSizeRatio() {
    return cheapMaxSizeRatio;
  }

  /**
   * Limit the maximum ratio of size between a candidate segment (that is deemed cheap) and the sum
   * of all smaller segments. Example: If it's 2.0, a 20KB segment will merge with a 10KB
   * segment (or say two 5KB segments) but will not with a 9KB segment.
   * The limit should be tuned if {@link #setCheapMinMergeAtOnce(int)} is changed. It should be more
   * than 1/(min-1).
   */
  public EagerCheapMergePolicy setCheapMaxSizeRatio(double cheapMaxSizeRatio) {
    if (cheapMaxSizeRatio <= 0.0) {
      throw new IllegalArgumentException("must be > 0");
    }
    this.cheapMaxSizeRatio = cheapMaxSizeRatio;
    return this;
  }

  /** Returns {@link #setCheapLimitConcurrentMergeSegments(int)}. */
  public int getCheapLimitConcurrentMergeSegments() {
    return cheapLimitConcurrentMergeSegments;
  }

  /**
   * Only find a cheap merge when the number of existing merging segments is less than or equal to
   * this number.  In all likelihood, those segments are being merged concurrently. By default this
   * setting is 0, thus won't find cheap merges during concurrent merging. If a higher number is
   * used (e.g. 10) then there is a likely chance {@link org.apache.lucene.index.ConcurrentMergeScheduler}
   * will intentionally stall the current thread if it's thresholds have been reached.  This may be
   * okay if there are multiple merge threads (thus less likely to reach such thresholds) or if it's
   * deemed more important to do cheap merges at the expense of slowing index throughput further.
   */
  public EagerCheapMergePolicy setCheapLimitConcurrentMergeSegments(int cheapLimitConcurrentMergeSegments) {
    this.cheapLimitConcurrentMergeSegments = cheapLimitConcurrentMergeSegments;
    return this;
  }

  @Override
  public String toString() {
    return super.toString() +
        ", cheapMergeThresholdBytes=" + cheapMergeThresholdBytes +
        ", cheapMaxSizeRatio=" + cheapMaxSizeRatio +
        ", cheapMinMergeAtOnce=" + cheapMinMergeAtOnce +
        ", cheapLimitConcurrentMergeSegments=" + cheapLimitConcurrentMergeSegments;
  }

  @Override
  public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
    InfoStream infoStream = mergeContext.getInfoStream();
    boolean verbose = infoStream.isEnabled(INFO_STREAM_CATEGORY);

    if (mergeTrigger == MergeTrigger.FULL_FLUSH) { // i.e. commit
      if (mergeContext.getMergingSegments().size() <= cheapLimitConcurrentMergeSegments) {
        OneMerge cheapMerge = findCheapMerge(segmentInfos, mergeContext);
        if (cheapMerge != null) {
          if (verbose) {
            infoStream.message(INFO_STREAM_CATEGORY,
                "Found a cheap merge: " + toSummaryString(cheapMerge.segments));
            //FYI further IW infoStream message "registerMerge" will have segment details
          }
          MergeSpecification spec = new MergeSpecification();
          spec.add(cheapMerge);
          return spec;
        }
      } else {
        if (verbose) {
          infoStream.message(INFO_STREAM_CATEGORY,
              "Don't look for a cheap merge because " +
                  mergeContext.getMergingSegments().size() + " segments are already merging.");
        }
      }
    }
    return super.findMerges(mergeTrigger, segmentInfos, mergeContext); // note: could still be null
  }

  private String toSummaryString(List<SegmentCommitInfo> segments) {
    long sizeBytes = segments.stream().mapToLong(EagerCheapMergePolicy::size).sum();
    return String.format(Locale.ROOT, "segCount=%d sizeMB=%.3f", segments.size(), sizeBytes / 1024.0 / 1024.0);
  }

  private OneMerge findCheapMerge(SegmentInfos infos, MergeContext mergeContext) throws IOException {
    List<SegmentCommitInfo> cheapMerge = findCheapMerge(infos.asList(), mergeContext.getMergingSegments());
    return cheapMerge == null ? null : new OneMerge(cheapMerge);
  }

  //@VisibleForTesting
  List<SegmentCommitInfo> findCheapMerge(List<SegmentCommitInfo> segmentInfoPerCommits,
                                         Collection<SegmentCommitInfo> alreadyMerging) {

    // Find segments that are below the size threshold and that aren't being merged already
    final List<SegmentCommitInfo> infosSortedAsc = segmentInfoPerCommits.stream()
        .filter(o -> !alreadyMerging.contains(o))
        .filter(o -> size(o) <= cheapMergeThresholdBytes)
        .sorted(Comparator.comparingLong(EagerCheapMergePolicy::size))
        .limit(getMaxMergeAtOnce())
        .collect(Collectors.toList());

    // Maximize how many of these we can merge while keeping in budget
    long accumSize = 0L;
    int numSegmentsToMerge = 0;
    for (int segIdx = 0; segIdx < infosSortedAsc.size(); segIdx++) {
      SegmentCommitInfo infoPerCommit = infosSortedAsc.get(segIdx);
      long segSize = size(infoPerCommit);
      if (accumSize + segSize > cheapMergeThresholdBytes) {
        break;
      }
      // if the ratio of this segment's size to the sum of sizes of segments we've seen is less than
      //   a threshold, then decide we will merge this (along with all before).
      // note: on 1st loop, accumSize==0; division results in: Infinity < whatever (false)
      if (((double) (segSize)) / ((double) accumSize) < cheapMaxSizeRatio) {
        numSegmentsToMerge = segIdx + 1;
      }
      accumSize += segSize;
    }
    if (numSegmentsToMerge < cheapMinMergeAtOnce) {
      return null; // too few segments to merge
    }
    return infosSortedAsc.subList(0, numSegmentsToMerge);
  }

  private static long size(SegmentCommitInfo infoPerCommit) {
    //note: We could discount for deleted docs (as TieredMergePolicy does) but lets keep this simple/cheap.
    try {
      return infoPerCommit.sizeInBytes();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}