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


import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;

/** This {@link MergePolicy} is used for upgrading all existing segments of
  * an index when calling {@link IndexWriter#forceMerge(int)}.
  * All other methods delegate to the base {@code MergePolicy} given to the constructor.
  * This allows for an as-cheap-as possible upgrade of an older index by only upgrading segments that
  * are created by previous Lucene versions. forceMerge in part still delegates to the wrapped {@code MergePolicy};
  * It will ask the wrapped policy for segments to merge, and the left over segments will be rewritten with the latest version 
  * (i.e. merged with themselves). 
  * <p>In general one would use {@link IndexUpgrader}, but for a fully customizeable upgrade,
  * you can use this like any other {@code MergePolicy} and call {@link IndexWriter#forceMerge(int)}:
  * <pre class="prettyprint lang-java">
  *  IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_XX, new KeywordAnalyzer());
  *  iwc.setMergePolicy(new UpgradeIndexMergePolicy(iwc.getMergePolicy()));
  *  IndexWriter w = new IndexWriter(dir, iwc);
  *  w.forceMerge(1);
  *  w.close();
  * </pre>
  * <p> The above example would result in a single segment in the latest version. However take this scenario: 
  * If there were 10 segments in the index and they all need upgrade, calling w.forceMerge(10) would leave 10 segments
  * in the index written with the latest lucene version. Calling w.forceMerge(5) would delegate wrapped merge policy
  * to determine which segments should be merged together, the remaining segments will be upgraded (rewritten) if need be.
  * <p><b>Warning:</b> This merge policy may reorder documents if the index was partially
  * upgraded before calling forceMerge (e.g., documents were added). If your application relies
  * on &quot;monotonicity&quot; of doc IDs (which means that the order in which the documents
  * were added to the index is preserved), do a forceMerge(1) instead. Please note, the
  * delegate {@code MergePolicy} may also reorder documents.
  * @lucene.experimental
  * @see IndexUpgrader
  */
public class UpgradeIndexMergePolicy extends MergePolicyWrapper {
  
  private int maxUpgradesAtATime = 5;
  private volatile boolean upgradeInProgress = false;
  private volatile boolean requireExplicitUpgrades = false;
  private boolean ignoreNewSegments = false;

  /** Wrap the given {@link MergePolicy} and intercept forceMerge requests to
   * only upgrade segments written with previous Lucene versions. */
  public UpgradeIndexMergePolicy(MergePolicy in) {
    super(in);
  }
  
  /**
   * Sets whether an explicit call to {@link #setUpgradeInProgress(boolean)} must 
   * be called before {@link #findForcedMerges(SegmentInfos, int, Map, IndexWriter)} in order for
   * an upgrade to initiate. Otherwise every request for a force merge will trigger and upgrade investigation
   * 
   * This option is recommended if using UpgradeIndexMergePolicy as the default merge policy and fine grained control
   * over when an upgrade is initiated is required
   * 
   * @param requireExplicitUpgrades whether or not setting upgrades in progress is required: Default false
   */
  public void setRequireExplicitUpgrades(boolean requireExplicitUpgrades) {
    this.requireExplicitUpgrades = requireExplicitUpgrades;
  }
  
  /**
   * Set whether or not it is ok for this merge policy to do an upgrade. This
   * option needs to enabled before doing a force merge for an upgrade to initiate. 
   * 
   * This option has no effect when {@code requireExplicitUpgrades} is disabled
   * 
   * @param upgradeInProgress allow this policy to upgrade segments: Default false
   */
  public void setUpgradeInProgress(boolean upgradeInProgress) {
    this.upgradeInProgress = upgradeInProgress;
  }
  
  /**
   * How many segment upgrades should be commited for scheduling at a time. If more segments
   * than maxUpgradeSegments need to be upgraded this merge policy relies on IndexWriters cascaded
   * requests to find segments to merge. Submitting a few segments at a time allows segments in need
   * of an upgrade to remain candidates for a natrually triggered merge.   
   *  
   * @param maxUpgradesAtATime how many segment upgrades should be commited for scheduling at a time: Default 5
   */
  public void setMaxUpgradesAtATime(int maxUpgradesAtATime) {
    this.maxUpgradesAtATime = maxUpgradesAtATime;
  }
  
  /**
   * @param ignoreNewSegments Whether or not this merge policy should ignore already upgraded segments when force merging: Default false
   */
  public void setIgnoreNewSegments(boolean ignoreNewSegments) {
    this.ignoreNewSegments  = ignoreNewSegments;
  }
  
  /** Returns if the given segment should be upgraded. The default implementation
   * will return {@code !Version.LATEST.equals(si.getVersion())},
   * so all segments created with a different version number than this Lucene version will
   * get upgraded.
   */
  protected boolean shouldUpgradeSegment(SegmentCommitInfo si) {
    return !Version.LATEST.equals(si.info.getVersion());
  }
  
  @Override
  public MergeSpecification findForcedMerges(SegmentInfos segmentInfos, int maxSegmentCount, Map<SegmentCommitInfo,Boolean> segmentsToMerge, IndexWriter writer) throws IOException {
    
    // Find segments to merge in directory, unless we are ignoring 
    // newer segments. If new segments are ignored, first old
    // segments need to be discovered.
    MergeSpecification spec = ignoreNewSegments ? null : in.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, writer);
    
    if(upgradeInProgress || !requireExplicitUpgrades) {
     
      // first find all old segments
      final Map<SegmentCommitInfo,Boolean> oldSegments = findSegmentsNeedingUpgrade(segmentInfos, segmentsToMerge);
      
      if (verbose(writer)) {
        message("findForcedMerges: segmentsToUpgrade=" + oldSegments, writer);
      }
        
      if (oldSegments.isEmpty()) {
        upgradeInProgress = false;  // Nothing to upgrade
        return spec;
      }
      
      if(ignoreNewSegments) {
        // Ask the wrapped spec now to do the merge with the old segments
        spec = in.findForcedMerges(segmentInfos, maxSegmentCount, oldSegments, writer);
      }
        
      if (spec != null) {
        // remove all segments that are in merge specification from oldSegments,
        // the resulting set contains all segments that are left over
        // and will be rewritten
        for (final OneMerge om : spec.merges) {
          oldSegments.keySet().removeAll(om.segments);
        }
      }
      
      // Add other segments missed by the wrapped merge policy to be upgraded
      return maybeUpdateSpecAndUpgradeProgress(spec, oldSegments, segmentInfos, writer);

    }

    return spec;
  }
  
  /**
   * Updates the the merge spec with old segments needing upgrade. Also sets whether or not to the upgrade needs to continue (upgradeInProgress=false)
   * 
   * @param spec the MergeSpecification to update
   * @param oldSegments the segments needing upgrade
   * @param segmentInfos all segment infos
   * @param writer the index writer
   * @return the possibly updated MergeSpecification
   */
  private MergeSpecification maybeUpdateSpecAndUpgradeProgress(MergeSpecification spec, Map<SegmentCommitInfo,Boolean> oldSegments, SegmentInfos segmentInfos, IndexWriter writer) {   
    if (!oldSegments.isEmpty()) {
      if (verbose(writer)) {
        message("findForcedMerges: " +  in.getClass().getSimpleName() +
        " does not want to merge all old segments, rewrite remaining ones into upgraded segments: " + oldSegments, writer);
      }
      
      if (spec == null) {
        spec = new MergeSpecification();
      }
      
      final int numWrappedSpecMerges = spec.merges.size();
      
      for (SegmentCommitInfo si: segmentInfos) {
        
        if(!oldSegments.containsKey(si)) {
          continue;
        }
        
        spec.add(new OneMerge(Collections.singletonList(si)));
        
        if((spec.merges.size() - numWrappedSpecMerges) == maxUpgradesAtATime) {
          return spec;
        }
        
      }
      
      // We found  we have less than the max number but greater than 0
      if(spec.merges.size() > numWrappedSpecMerges) {
          return spec;
      }
      
    }
    
    // Only set this once there are 0 segments needing upgrading, because when we return a
    // spec, IndexWriter may (silently!) reject that merge if some of the segments we asked
    // to be merged were already being (naturally) merged:
    upgradeInProgress = false;
    
    return spec;
  }
  
  private Map<SegmentCommitInfo,Boolean> findSegmentsNeedingUpgrade(SegmentInfos segmentInfos, Map<SegmentCommitInfo,Boolean> segmentsToMerge) {
    final Map<SegmentCommitInfo,Boolean> oldSegments = new HashMap<>();
    
    for (final SegmentCommitInfo si : segmentInfos) {
      final Boolean v = segmentsToMerge.get(si);
      if (v != null && shouldUpgradeSegment(si)) {
        oldSegments.put(si, v);
      }
    }
    
    return oldSegments;
  }
  
  private boolean verbose(IndexWriter writer) {
    return writer != null && writer.infoStream.isEnabled("UPGMP");
  }

  private void message(String message, IndexWriter writer) {
    writer.infoStream.message("UPGMP", message);
  }
}
