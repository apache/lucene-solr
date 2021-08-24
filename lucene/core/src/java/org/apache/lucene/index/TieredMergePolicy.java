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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 *  Merges segments of approximately equal size, subject to
 *  an allowed number of segments per tier.  This is similar
 *  to {@link LogByteSizeMergePolicy}, except this merge
 *  policy is able to merge non-adjacent segment, and
 *  separates how many segments are merged at once ({@link
 *  #setMaxMergeAtOnce}) from how many segments are allowed
 *  per tier ({@link #setSegmentsPerTier}).  This merge
 *  policy also does not over-merge (i.e. cascade merges). 
 *
 *  <p>For normal merging, this policy first computes a
 *  "budget" of how many segments are allowed to be in the
 *  index.  If the index is over-budget, then the policy
 *  sorts segments by decreasing size (pro-rating by percent
 *  deletes), and then finds the least-cost merge.  Merge
 *  cost is measured by a combination of the "skew" of the
 *  merge (size of largest segment divided by smallest segment),
 *  total merge size and percent deletes reclaimed,
 *  so that merges with lower skew, smaller size
 *  and those reclaiming more deletes, are
 *  favored.
 *
 *  <p>If a merge will produce a segment that's larger than
 *  {@link #setMaxMergedSegmentMB}, then the policy will
 *  merge fewer segments (down to 1 at once, if that one has
 *  deletions) to keep the segment size under budget.
 *      
 *  <p><b>NOTE</b>: this policy freely merges non-adjacent
 *  segments; if this is a problem, use {@link
 *  LogMergePolicy}.
 *
 *  <p><b>NOTE</b>: This policy always merges by byte size
 *  of the segments, always pro-rates by percent deletes
 *
 *  <p><b>NOTE</b> Starting with Lucene 7.5, there are several changes:
 *
 *  - findForcedMerges and findForcedDeletesMerges) respect the max segment
 *  size by default.
 *
 *  - When findforcedmerges is called with maxSegmentCount other than 1,
 *  the resulting index is not guaranteed to have &lt;= maxSegmentCount segments.
 *  Rather it is on a "best effort" basis. Specifically the theoretical ideal
 *  segment size is calculated and a "fudge factor" of 25% is added as the
 *  new maxSegmentSize, which is respected.
 *
 *  - findForcedDeletesMerges will not produce segments greater than
 *  maxSegmentSize.
 *
 *  @lucene.experimental
 */

// TODO
//   - we could try to take into account whether a large
//     merge is already running (under CMS) and then bias
//     ourselves towards picking smaller merges if so (or,
//     maybe CMS should do so)

public class TieredMergePolicy extends MergePolicy {
  /** Default noCFSRatio.  If a merge's size is {@code >= 10%} of
   *  the index, then we disable compound file for it.
   *  @see MergePolicy#setNoCFSRatio */
  public static final double DEFAULT_NO_CFS_RATIO = 0.1;

  // User-specified maxMergeAtOnce. In practice we always take the min of its
  // value and segsPerTier to avoid suboptimal merging.
  private int maxMergeAtOnce = 10;
  private long maxMergedSegmentBytes = 5*1024*1024*1024L;
  private int maxMergeAtOnceExplicit = Integer.MAX_VALUE;

  private long floorSegmentBytes = 2*1024*1024L;
  private double segsPerTier = 10.0;
  private double forceMergeDeletesPctAllowed = 10.0;
  private double deletesPctAllowed = 33.0;

  /** Sole constructor, setting all settings to their
   *  defaults. */
  public TieredMergePolicy() {
    super(DEFAULT_NO_CFS_RATIO, MergePolicy.DEFAULT_MAX_CFS_SEGMENT_SIZE);
  }

  /** Maximum number of segments to be merged at a time
   *  during "normal" merging.  For explicit merging (eg,
   *  forceMerge or forceMergeDeletes was called), see {@link
   *  #setMaxMergeAtOnceExplicit}.  Default is 10. */
  public TieredMergePolicy setMaxMergeAtOnce(int v) {
    if (v < 2) {
      throw new IllegalArgumentException("maxMergeAtOnce must be > 1 (got " + v + ")");
    }
    maxMergeAtOnce = v;
    return this;
  }

  private enum MERGE_TYPE {
    NATURAL, FORCE_MERGE, FORCE_MERGE_DELETES
  }
  /** Returns the current maxMergeAtOnce setting.
   *
   * @see #setMaxMergeAtOnce */
  public int getMaxMergeAtOnce() {
    return maxMergeAtOnce;
  }

  // TODO: should addIndexes do explicit merging, too?  And,
  // if user calls IW.maybeMerge "explicitly"

  /** Maximum number of segments to be merged at a time,
   *  during forceMerge or forceMergeDeletes. Default is unlimited.
   * @deprecated This method will be removed in Lucene 9 and explicit
   *             merges won't limit the number of merged segments. */
  @Deprecated
  public TieredMergePolicy setMaxMergeAtOnceExplicit(int v) {
    if (v < 2) {
      throw new IllegalArgumentException("maxMergeAtOnceExplicit must be > 1 (got " + v + ")");
    }
    maxMergeAtOnceExplicit = v;
    return this;
  }


  /** Returns the current maxMergeAtOnceExplicit setting.
   *
   * @see #setMaxMergeAtOnceExplicit
   * @deprecated This method will be removed in Lucene 9. */
  @Deprecated
  public int getMaxMergeAtOnceExplicit() {
    return maxMergeAtOnceExplicit;
  }

  /** Maximum sized segment to produce during
   *  normal merging.  This setting is approximate: the
   *  estimate of the merged segment size is made by summing
   *  sizes of to-be-merged segments (compensating for
   *  percent deleted docs).  Default is 5 GB. */
  public TieredMergePolicy setMaxMergedSegmentMB(double v) {
    if (v < 0.0) {
      throw new IllegalArgumentException("maxMergedSegmentMB must be >=0 (got " + v + ")");
    }
    v *= 1024 * 1024;
    maxMergedSegmentBytes = v > Long.MAX_VALUE ? Long.MAX_VALUE : (long) v;
    return this;
  }

  /** Returns the current maxMergedSegmentMB setting.
   *
   * @see #setMaxMergedSegmentMB */
  public double getMaxMergedSegmentMB() {
    return maxMergedSegmentBytes/1024.0/1024.0;
  }

  /** Controls the maximum percentage of deleted documents that is tolerated in
   *  the index. Lower values make the index more space efficient at the
   *  expense of increased CPU and I/O activity. Values must be between 20 and
   *  50. Default value is 33. */
  public TieredMergePolicy setDeletesPctAllowed(double v) {
    if (v < 20 || v > 50) {
      throw new IllegalArgumentException("indexPctDeletedTarget must be >= 20.0 and <= 50 (got " + v + ")");
    }
    deletesPctAllowed = v;
    return this;
  }

  /** Returns the current deletesPctAllowed setting.
   *
   * @see #setDeletesPctAllowed */
  public double getDeletesPctAllowed() {
    return deletesPctAllowed;
  }

  /** Segments smaller than this are "rounded up" to this
   *  size, ie treated as equal (floor) size for merge
   *  selection.  This is to prevent frequent flushing of
   *  tiny segments from allowing a long tail in the index.
   *  Default is 2 MB. */
  public TieredMergePolicy setFloorSegmentMB(double v) {
    if (v <= 0.0) {
      throw new IllegalArgumentException("floorSegmentMB must be > 0.0 (got " + v + ")");
    }
    v *= 1024 * 1024;
    floorSegmentBytes = v > Long.MAX_VALUE ? Long.MAX_VALUE : (long) v;
    return this;
  }

  /** Returns the current floorSegmentMB.
   *
   *  @see #setFloorSegmentMB */
  public double getFloorSegmentMB() {
    return floorSegmentBytes/(1024*1024.);
  }

  /** When forceMergeDeletes is called, we only merge away a
   *  segment if its delete percentage is over this
   *  threshold.  Default is 10%. */ 
  public TieredMergePolicy setForceMergeDeletesPctAllowed(double v) {
    if (v < 0.0 || v > 100.0) {
      throw new IllegalArgumentException("forceMergeDeletesPctAllowed must be between 0.0 and 100.0 inclusive (got " + v + ")");
    }
    forceMergeDeletesPctAllowed = v;
    return this;
  }

  /** Returns the current forceMergeDeletesPctAllowed setting.
   *
   * @see #setForceMergeDeletesPctAllowed */
  public double getForceMergeDeletesPctAllowed() {
    return forceMergeDeletesPctAllowed;
  }

  /** Sets the allowed number of segments per tier.  Smaller
   *  values mean more merging but fewer segments.
   *
   *  <p>Default is 10.0.</p> */
  public TieredMergePolicy setSegmentsPerTier(double v) {
    if (v < 2.0) {
      throw new IllegalArgumentException("segmentsPerTier must be >= 2.0 (got " + v + ")");
    }
    segsPerTier = v;
    return this;
  }

  /** Returns the current segmentsPerTier setting.
   *
   * @see #setSegmentsPerTier */
  public double getSegmentsPerTier() {
    return segsPerTier;
  }

  private static class SegmentSizeAndDocs {
    private final SegmentCommitInfo segInfo;
    private final long sizeInBytes;
    private final int delCount;
    private final int maxDoc;
    private final String name;

    SegmentSizeAndDocs(SegmentCommitInfo info, final long sizeInBytes, final int segDelCount) throws IOException {
      segInfo = info;
      this.name = info.info.name;
      this.sizeInBytes = sizeInBytes;
      this.delCount = segDelCount;
      this.maxDoc = info.info.maxDoc();
    }
  }

  /** Holds score and explanation for a single candidate
   *  merge. */
  protected static abstract class MergeScore {
    /** Sole constructor. (For invocation by subclass 
     *  constructors, typically implicit.) */
    protected MergeScore() {
    }
    
    /** Returns the score for this merge candidate; lower
     *  scores are better. */
    abstract double getScore();

    /** Human readable explanation of how the merge got this
     *  score. */
    abstract String getExplanation();
  }


  // The size can change concurrently while we are running here, because deletes
  // are now applied concurrently, and this can piss off TimSort!  So we
  // call size() once per segment and sort by that:

  private List<SegmentSizeAndDocs> getSortedBySegmentSize(final SegmentInfos infos, final MergeContext mergeContext) throws IOException {
    List<SegmentSizeAndDocs> sortedBySize = new ArrayList<>();

    for (SegmentCommitInfo info : infos) {
      sortedBySize.add(new SegmentSizeAndDocs(info, size(info, mergeContext), mergeContext.numDeletesToMerge(info)));
    }

    sortedBySize.sort((o1, o2) -> {
      // Sort by largest size:
      int cmp = Long.compare(o2.sizeInBytes, o1.sizeInBytes);
      if (cmp == 0) {
        cmp = o1.name.compareTo(o2.name);
      }
      return cmp;

    });

    return sortedBySize;
  }


  @Override
  public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext) throws IOException {
    final Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();
    // Compute total index bytes & print details about the index
    long totIndexBytes = 0;
    long minSegmentBytes = Long.MAX_VALUE;

    int totalDelDocs = 0;
    int totalMaxDoc = 0;

    long mergingBytes = 0;

    List<SegmentSizeAndDocs> sortedInfos = getSortedBySegmentSize(infos, mergeContext);
    Iterator<SegmentSizeAndDocs> iter = sortedInfos.iterator();
    while (iter.hasNext()) {
      SegmentSizeAndDocs segSizeDocs = iter.next();
      final long segBytes = segSizeDocs.sizeInBytes;
      if (verbose(mergeContext)) {
        String extra = merging.contains(segSizeDocs.segInfo) ? " [merging]" : "";
        if (segBytes >= maxMergedSegmentBytes) {
          extra += " [skip: too large]";
        } else if (segBytes < floorSegmentBytes) {
          extra += " [floored]";
        }
        message("  seg=" + segString(mergeContext, Collections.singleton(segSizeDocs.segInfo)) + " size=" + String.format(Locale.ROOT, "%.3f", segBytes / 1024 / 1024.) + " MB" + extra, mergeContext);
      }
      if (merging.contains(segSizeDocs.segInfo)) {
        mergingBytes += segSizeDocs.sizeInBytes;
        iter.remove();
        // if this segment is merging, then its deletes are being reclaimed already.
        // only count live docs in the total max doc
        totalMaxDoc += segSizeDocs.maxDoc - segSizeDocs.delCount;
      } else {
        totalDelDocs += segSizeDocs.delCount;
        totalMaxDoc += segSizeDocs.maxDoc;
      }

      minSegmentBytes = Math.min(segBytes, minSegmentBytes);
      totIndexBytes += segBytes;
    }
    assert totalMaxDoc >= 0;
    assert totalDelDocs >= 0;

    final double totalDelPct = 100 * (double) totalDelDocs / totalMaxDoc;
    int allowedDelCount = (int) (deletesPctAllowed * totalMaxDoc / 100);

    // If we have too-large segments, grace them out of the maximum segment count
    // If we're above certain thresholds of deleted docs, we can merge very large segments.
    int tooBigCount = 0;
    iter = sortedInfos.iterator();

    // remove large segments from consideration under two conditions.
    // 1> Overall percent deleted docs relatively small and this segment is larger than 50% maxSegSize
    // 2> overall percent deleted docs large and this segment is large and has few deleted docs

    while (iter.hasNext()) {
      SegmentSizeAndDocs segSizeDocs = iter.next();
      double segDelPct = 100 * (double) segSizeDocs.delCount / (double) segSizeDocs.maxDoc;
      if (segSizeDocs.sizeInBytes > maxMergedSegmentBytes / 2 && (totalDelPct <= deletesPctAllowed || segDelPct <= deletesPctAllowed)) {
        iter.remove();
        tooBigCount++; // Just for reporting purposes.
        totIndexBytes -= segSizeDocs.sizeInBytes;
        allowedDelCount -= segSizeDocs.delCount;
      }
    }
    allowedDelCount = Math.max(0, allowedDelCount);

    final int mergeFactor = (int) Math.min(maxMergeAtOnce, segsPerTier);
    // Compute max allowed segments in the index
    long levelSize = Math.max(minSegmentBytes, floorSegmentBytes);
    long bytesLeft = totIndexBytes;
    double allowedSegCount = 0;
    while (true) {
      final double segCountLevel = bytesLeft / (double) levelSize;
      if (segCountLevel < segsPerTier || levelSize == maxMergedSegmentBytes) {
        allowedSegCount += Math.ceil(segCountLevel);
        break;
      }
      allowedSegCount += segsPerTier;
      bytesLeft -= segsPerTier * levelSize;
      levelSize = Math.min(maxMergedSegmentBytes, levelSize * mergeFactor);
    }
    // allowedSegCount may occasionally be less than segsPerTier
    // if segment sizes are below the floor size
    allowedSegCount = Math.max(allowedSegCount, segsPerTier);

    if (verbose(mergeContext) && tooBigCount > 0) {
      message("  allowedSegmentCount=" + allowedSegCount + " vs count=" + infos.size() +
          " (eligible count=" + sortedInfos.size() + ") tooBigCount= " + tooBigCount, mergeContext);
    }
    return doFindMerges(sortedInfos, maxMergedSegmentBytes, mergeFactor, (int) allowedSegCount, allowedDelCount, MERGE_TYPE.NATURAL,
        mergeContext, mergingBytes >= maxMergedSegmentBytes);
  }

  private MergeSpecification doFindMerges(List<SegmentSizeAndDocs> sortedEligibleInfos,
                                          final long maxMergedSegmentBytes,
                                          final int mergeFactor, final int allowedSegCount,
                                          final int allowedDelCount, final MERGE_TYPE mergeType,
                                          MergeContext mergeContext,
                                          boolean maxMergeIsRunning) throws IOException {

    List<SegmentSizeAndDocs> sortedEligible = new ArrayList<>(sortedEligibleInfos);

    Map<SegmentCommitInfo, SegmentSizeAndDocs> segInfosSizes = new HashMap<>();
    for (SegmentSizeAndDocs segSizeDocs : sortedEligible) {
      segInfosSizes.put(segSizeDocs.segInfo, segSizeDocs);
    }

    int originalSortedSize = sortedEligible.size();
    if (verbose(mergeContext)) {
      message("findMerges: " + originalSortedSize + " segments", mergeContext);
    }
    if (originalSortedSize == 0) {
      return null;
    }

    final Set<SegmentCommitInfo> toBeMerged = new HashSet<>();

    MergeSpecification spec = null;

    // Cycle to possibly select more than one merge:
    // The trigger point for total deleted documents in the index leads to a bunch of large segment
    // merges at the same time. So only put one large merge in the list of merges per cycle. We'll pick up another
    // merge next time around.
    boolean haveOneLargeMerge = false;

    while (true) {

      // Gather eligible segments for merging, ie segments
      // not already being merged and not already picked (by
      // prior iteration of this loop) for merging:

      // Remove ineligible segments. These are either already being merged or already picked by prior iterations
      Iterator<SegmentSizeAndDocs> iter = sortedEligible.iterator();
      while (iter.hasNext()) {
        SegmentSizeAndDocs segSizeDocs = iter.next();
        if (toBeMerged.contains(segSizeDocs.segInfo)) {
          iter.remove();
        }
      }

      if (verbose(mergeContext)) {
        message("  allowedSegmentCount=" + allowedSegCount + " vs count=" + originalSortedSize + " (eligible count=" + sortedEligible.size() + ")", mergeContext);
      }

      if (sortedEligible.size() == 0) {
        return spec;
      }

      final int remainingDelCount = sortedEligible.stream().mapToInt(c -> c.delCount).sum();
      if (mergeType == MERGE_TYPE.NATURAL &&
          sortedEligible.size() <= allowedSegCount &&
          remainingDelCount <= allowedDelCount) {
        return spec;
      }

      // OK we are over budget -- find best merge!
      MergeScore bestScore = null;
      List<SegmentCommitInfo> best = null;
      boolean bestTooLarge = false;
      long bestMergeBytes = 0;

      for (int startIdx = 0; startIdx < sortedEligible.size(); startIdx++) {

        long totAfterMergeBytes = 0;

        final List<SegmentCommitInfo> candidate = new ArrayList<>();
        boolean hitTooLarge = false;
        long bytesThisMerge = 0;
        for (int idx = startIdx; idx < sortedEligible.size() && candidate.size() < mergeFactor && bytesThisMerge < maxMergedSegmentBytes; idx++) {
          final SegmentSizeAndDocs segSizeDocs = sortedEligible.get(idx);
          final long segBytes = segSizeDocs.sizeInBytes;

          if (totAfterMergeBytes + segBytes > maxMergedSegmentBytes) {
            hitTooLarge = true;
            if (candidate.size() == 0) {
              // We should never have something coming in that _cannot_ be merged, so handle singleton merges
              candidate.add(segSizeDocs.segInfo);
              bytesThisMerge += segBytes;
            }
            // NOTE: we continue, so that we can try
            // "packing" smaller segments into this merge
            // to see if we can get closer to the max
            // size; this in general is not perfect since
            // this is really "bin packing" and we'd have
            // to try different permutations.
            continue;
          }
          candidate.add(segSizeDocs.segInfo);
          bytesThisMerge += segBytes;
          totAfterMergeBytes += segBytes;
        }

        // We should never see an empty candidate: we iterated over maxMergeAtOnce
        // segments, and already pre-excluded the too-large segments:
        assert candidate.size() > 0;

        // A singleton merge with no deletes makes no sense. We can get here when forceMerge is looping around...
        if (candidate.size() == 1) {
          SegmentSizeAndDocs segSizeDocs = segInfosSizes.get(candidate.get(0));
          if (segSizeDocs.delCount == 0) {
            continue;
          }
        }

        // If we didn't find a too-large merge and have a list of candidates
        // whose length is less than the merge factor, it means we are reaching
        // the tail of the list of segments and will only find smaller merges.
        // Stop here.
        if (bestScore != null &&
            hitTooLarge == false &&
            candidate.size() < mergeFactor) {
          break;
        }

        final MergeScore score = score(candidate, hitTooLarge, segInfosSizes);
        if (verbose(mergeContext)) {
          message("  maybe=" + segString(mergeContext, candidate) + " score=" + score.getScore() + " " + score.getExplanation() + " tooLarge=" + hitTooLarge + " size=" + String.format(Locale.ROOT, "%.3f MB", totAfterMergeBytes/1024./1024.), mergeContext);
        }

        if ((bestScore == null || score.getScore() < bestScore.getScore()) && (!hitTooLarge || !maxMergeIsRunning)) {
          best = candidate;
          bestScore = score;
          bestTooLarge = hitTooLarge;
          bestMergeBytes = totAfterMergeBytes;
        }
      }

      if (best == null) {
        return spec;
      }
      // The mergeType == FORCE_MERGE_DELETES behaves as the code does currently and can create a large number of
      // concurrent big merges. If we make findForcedDeletesMerges behave as findForcedMerges and cycle through
      // we should remove this.
      if (haveOneLargeMerge == false || bestTooLarge == false || mergeType == MERGE_TYPE.FORCE_MERGE_DELETES) {

        haveOneLargeMerge |= bestTooLarge;

        if (spec == null) {
          spec = new MergeSpecification();
        }
        final OneMerge merge = new OneMerge(best);
        spec.add(merge);

        if (verbose(mergeContext)) {
          message("  add merge=" + segString(mergeContext, merge.segments) + " size=" + String.format(Locale.ROOT, "%.3f MB", bestMergeBytes / 1024. / 1024.) + " score=" + String.format(Locale.ROOT, "%.3f", bestScore.getScore()) + " " + bestScore.getExplanation() + (bestTooLarge ? " [max merge]" : ""), mergeContext);
        }
      }
      // whether we're going to return this list in the spec of not, we need to remove it from
      // consideration on the next loop.
      toBeMerged.addAll(best);
    }
  }

  /** Expert: scores one merge; subclasses can override. */
  protected MergeScore score(List<SegmentCommitInfo> candidate, boolean hitTooLarge, Map<SegmentCommitInfo, SegmentSizeAndDocs> segmentsSizes) throws IOException {
    long totBeforeMergeBytes = 0;
    long totAfterMergeBytes = 0;
    long totAfterMergeBytesFloored = 0;
    for(SegmentCommitInfo info : candidate) {
      final long segBytes = segmentsSizes.get(info).sizeInBytes;
      totAfterMergeBytes += segBytes;
      totAfterMergeBytesFloored += floorSize(segBytes);
      totBeforeMergeBytes += info.sizeInBytes();
    }

    // Roughly measure "skew" of the merge, i.e. how
    // "balanced" the merge is (whether the segments are
    // about the same size), which can range from
    // 1.0/numSegsBeingMerged (good) to 1.0 (poor). Heavily
    // lopsided merges (skew near 1.0) is no good; it means
    // O(N^2) merge cost over time:
    final double skew;
    if (hitTooLarge) {
      // Pretend the merge has perfect skew; skew doesn't
      // matter in this case because this merge will not
      // "cascade" and so it cannot lead to N^2 merge cost
      // over time:
      final int mergeFactor = (int) Math.min(maxMergeAtOnce, segsPerTier);
      skew = 1.0/mergeFactor;
    } else {
      skew = ((double) floorSize(segmentsSizes.get(candidate.get(0)).sizeInBytes)) / totAfterMergeBytesFloored;
    }

    // Strongly favor merges with less skew (smaller
    // mergeScore is better):
    double mergeScore = skew;

    // Gently favor smaller merges over bigger ones.  We
    // don't want to make this exponent too large else we
    // can end up doing poor merges of small segments in
    // order to avoid the large merges:
    mergeScore *= Math.pow(totAfterMergeBytes, 0.05);

    // Strongly favor merges that reclaim deletes:
    final double nonDelRatio = ((double) totAfterMergeBytes)/totBeforeMergeBytes;
    mergeScore *= Math.pow(nonDelRatio, 2);

    final double finalMergeScore = mergeScore;

    return new MergeScore() {

      @Override
      public double getScore() {
        return finalMergeScore;
      }

      @Override
      public String getExplanation() {
        return "skew=" + String.format(Locale.ROOT, "%.3f", skew) + " nonDelRatio=" + String.format(Locale.ROOT, "%.3f", nonDelRatio);
      }
    };
  }

  @Override
  public MergeSpecification findForcedMerges(SegmentInfos infos, int maxSegmentCount, Map<SegmentCommitInfo, Boolean> segmentsToMerge, MergeContext mergeContext) throws IOException {
    if (verbose(mergeContext)) {
      message("findForcedMerges maxSegmentCount=" + maxSegmentCount + " infos=" + segString(mergeContext, infos) +
          " segmentsToMerge=" + segmentsToMerge, mergeContext);
    }

    List<SegmentSizeAndDocs> sortedSizeAndDocs = getSortedBySegmentSize(infos, mergeContext);

    long totalMergeBytes = 0;
    final Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();


    // Trim the list down, remove if we're respecting max segment size and it's not original. Presumably it's been merged before and
    //   is close enough to the max segment size we shouldn't add it in again.
    Iterator<SegmentSizeAndDocs> iter = sortedSizeAndDocs.iterator();
    boolean forceMergeRunning = false;
    while (iter.hasNext()) {
      SegmentSizeAndDocs segSizeDocs = iter.next();
      final Boolean isOriginal = segmentsToMerge.get(segSizeDocs.segInfo);
      if (isOriginal == null) {
        iter.remove();
      } else {
        if (merging.contains(segSizeDocs.segInfo)) {
          forceMergeRunning = true;
          iter.remove();
        } else {
          totalMergeBytes += segSizeDocs.sizeInBytes;
        }
      }
    }

    long maxMergeBytes = maxMergedSegmentBytes;

    // Set the maximum segment size based on how many segments have been specified.
    if (maxSegmentCount == 1) maxMergeBytes = Long.MAX_VALUE;
    else if (maxSegmentCount != Integer.MAX_VALUE) {
      // Fudge this up a bit so we have a better chance of not having to rewrite segments. If we use the exact size,
      // it's almost guaranteed that the segments won't fit perfectly and we'll be left with more segments than
      // we want and have to re-merge in the code at the bottom of this method.
      maxMergeBytes = Math.max((long) (((double) totalMergeBytes / (double) maxSegmentCount)), maxMergedSegmentBytes);
      maxMergeBytes = (long) ((double) maxMergeBytes * 1.25);
    }

    iter = sortedSizeAndDocs.iterator();
    boolean foundDeletes = false;
    while (iter.hasNext()) {
      SegmentSizeAndDocs segSizeDocs = iter.next();
      Boolean isOriginal = segmentsToMerge.get(segSizeDocs.segInfo);
      if (segSizeDocs.delCount != 0) { // This is forceMerge, all segments with deleted docs should be merged.
        if (isOriginal != null && isOriginal) {
          foundDeletes = true;
        }
        continue;
      }
      // Let the scoring handle whether to merge large segments.
      if (maxSegmentCount == Integer.MAX_VALUE && isOriginal != null && isOriginal == false) {
        iter.remove();
      }
      // Don't try to merge a segment with no deleted docs that's over the max size.
      if (maxSegmentCount != Integer.MAX_VALUE && segSizeDocs.sizeInBytes >= maxMergeBytes) {
        iter.remove();
      }
    }

    // Nothing to merge this round.
    if (sortedSizeAndDocs.size() == 0) {
      return null;
    }

    // We should never bail if there are segments that have deleted documents, all deleted docs should be purged.
    if (foundDeletes == false) {
      SegmentCommitInfo infoZero = sortedSizeAndDocs.get(0).segInfo;
      if ((maxSegmentCount != Integer.MAX_VALUE && maxSegmentCount > 1 && sortedSizeAndDocs.size() <= maxSegmentCount) ||
          (maxSegmentCount == 1 && sortedSizeAndDocs.size() == 1 && (segmentsToMerge.get(infoZero) != null || isMerged(infos, infoZero, mergeContext)))) {
        if (verbose(mergeContext)) {
          message("already merged", mergeContext);
        }
        return null;
      }
    }

    if (verbose(mergeContext)) {
      message("eligible=" + sortedSizeAndDocs, mergeContext);
    }

    final int startingSegmentCount = sortedSizeAndDocs.size();
    final boolean finalMerge = startingSegmentCount < maxSegmentCount + maxMergeAtOnceExplicit - 1;
    if (finalMerge && forceMergeRunning) {
      return null;
    }

    // This is the special case of merging down to one segment
    if (sortedSizeAndDocs.size() < maxMergeAtOnceExplicit && maxSegmentCount == 1 && totalMergeBytes < maxMergeBytes) {
      MergeSpecification spec = new MergeSpecification();
      List<SegmentCommitInfo> allOfThem = new ArrayList<>();
      for (SegmentSizeAndDocs segSizeDocs : sortedSizeAndDocs) {
        allOfThem.add(segSizeDocs.segInfo);
      }
      spec.add(new OneMerge(allOfThem));
      return spec;
    }

    MergeSpecification spec = null;

    int index = startingSegmentCount - 1;
    int resultingSegments = startingSegmentCount;
    while (true) {
      List<SegmentCommitInfo> candidate = new ArrayList<>();
      long currentCandidateBytes = 0L;
      int mergesAllowed = maxMergeAtOnceExplicit;
      while (index >= 0 && resultingSegments > maxSegmentCount && mergesAllowed > 0) {
        final SegmentCommitInfo current = sortedSizeAndDocs.get(index).segInfo;
        final int initialCandidateSize = candidate.size();
        final long currentSegmentSize = current.sizeInBytes();
        // We either add to the bin because there's space or because the it is the smallest possible bin since
        // decrementing the index will move us to even larger segments.
        if (currentCandidateBytes + currentSegmentSize <= maxMergeBytes || initialCandidateSize < 2) {
          candidate.add(current);
          --index;
          currentCandidateBytes += currentSegmentSize;
          --mergesAllowed;
          if (initialCandidateSize > 0) {
            // Any merge that handles two or more segments reduces the resulting number of segments
            // by the number of segments handled - 1
            --resultingSegments;
          }
        } else {
          break;
        }
      }
      final int candidateSize = candidate.size();
      // While a force merge is running, only merges that cover the maximum allowed number of segments or that create a segment close to the
      // maximum allowed segment sized are permitted
      if (candidateSize > 1 && (forceMergeRunning == false || candidateSize == maxMergeAtOnceExplicit || candidateSize > 0.7 * maxMergeBytes)) {
        final OneMerge merge = new OneMerge(candidate);
        if (verbose(mergeContext)) {
          message("add merge=" + segString(mergeContext, merge.segments), mergeContext);
        }
        if (spec == null) {
          spec = new MergeSpecification();
        }
        spec.add(merge);
      } else {
        return spec;
      }
    }
  }

  @Override
  public MergeSpecification findForcedDeletesMerges(SegmentInfos infos, MergeContext mergeContext) throws IOException {
    if (verbose(mergeContext)) {
      message("findForcedDeletesMerges infos=" + segString(mergeContext, infos) + " forceMergeDeletesPctAllowed=" + forceMergeDeletesPctAllowed, mergeContext);
    }

    // First do a quick check that there's any work to do.
    // NOTE: this makes BaseMergePOlicyTestCase.testFindForcedDeletesMerges work
    final Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();

    boolean haveWork = false;
    for(SegmentCommitInfo info : infos) {
      int delCount = mergeContext.numDeletesToMerge(info);
      assert assertDelCount(delCount, info);
      double pctDeletes = 100.*((double) delCount)/info.info.maxDoc();
      if (pctDeletes > forceMergeDeletesPctAllowed && !merging.contains(info)) {
        haveWork = true;
        break;
      }
    }

    if (haveWork == false) {
      return null;
    }

    List<SegmentSizeAndDocs> sortedInfos = getSortedBySegmentSize(infos, mergeContext);

    Iterator<SegmentSizeAndDocs> iter = sortedInfos.iterator();
    while (iter.hasNext()) {
      SegmentSizeAndDocs segSizeDocs = iter.next();
      double pctDeletes = 100. * ((double) segSizeDocs.delCount / (double) segSizeDocs.maxDoc);
      if (merging.contains(segSizeDocs.segInfo) || pctDeletes <= forceMergeDeletesPctAllowed) {
        iter.remove();
      }
    }

    if (verbose(mergeContext)) {
      message("eligible=" + sortedInfos, mergeContext);
    }
    return doFindMerges(sortedInfos, maxMergedSegmentBytes,
        maxMergeAtOnceExplicit, Integer.MAX_VALUE, 0, MERGE_TYPE.FORCE_MERGE_DELETES, mergeContext, false);

  }

  private long floorSize(long bytes) {
    return Math.max(floorSegmentBytes, bytes);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("[" + getClass().getSimpleName() + ": ");
    sb.append("maxMergeAtOnce=").append(maxMergeAtOnce).append(", ");
    sb.append("maxMergeAtOnceExplicit=").append(maxMergeAtOnceExplicit).append(", ");
    sb.append("maxMergedSegmentMB=").append(maxMergedSegmentBytes/1024/1024.).append(", ");
    sb.append("floorSegmentMB=").append(floorSegmentBytes/1024/1024.).append(", ");
    sb.append("forceMergeDeletesPctAllowed=").append(forceMergeDeletesPctAllowed).append(", ");
    sb.append("segmentsPerTier=").append(segsPerTier).append(", ");
    sb.append("maxCFSSegmentSizeMB=").append(getMaxCFSSegmentSizeMB()).append(", ");
    sb.append("noCFSRatio=").append(noCFSRatio).append(", ");
    sb.append("deletesPctAllowed=").append(deletesPctAllowed);
    return sb.toString();
  }
}
