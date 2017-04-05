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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.lucene.util.Version;

/** This {@link MergePolicy} is used for upgrading all existing segments of
  * an index when calling {@link IndexWriter#forceMerge(int)}.
  * All other methods delegate to the base {@code MergePolicy} given to the constructor.
  * Upgrades will only take place after enableUpgrades has been set to true.
  * When upgrades are enabled, it will ask the wrapped policy for segments to merge, and the left over segments will be 
  * rewritten with the latest version (i.e. merged with themselves).
  * If upgrades are not enabled upgrades are delegated to the wrapped merge policy.
  *  
  * <p>In general one would use {@link IndexUpgrader}, but for a fully customizeable upgrade,
  * you can use this like any other {@code MergePolicy} and call {@link IndexWriter#forceMerge(int)}:
  * <pre class="prettyprint lang-java">
  *  IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_XX, new KeywordAnalyzer());
  *  iwc.setMergePolicy(new LiveUpgradeIndexMergePolicy(iwc.getMergePolicy()));
  *  IndexWriter w = new IndexWriter(dir, iwc);
  *  w.setEnableUpgrades(true);
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
public class LiveUpgradeSegmentsMergePolicy extends MergePolicyWrapper {
  
  private int maxUpgradesAtATime = 5;
  private volatile boolean enableUpgrades = false;
  private Predicate<SegmentCommitInfo> shouldSegmentUpgrade = LiveUpgradeSegmentsMergePolicy::segmentIsLatestVersion; 

  /** Wrap the given {@link MergePolicy} and intercept forceMerge requests to
   * only upgrade segments written with previous Lucene versions. */
  public LiveUpgradeSegmentsMergePolicy(MergePolicy in) {
    super(in);
  }
    
  /**
   * Set whether or not it is ok for this merge policy to do an upgrade. 
   * When true this merge policy will look for segments to upgrade.
   * 
   * When all upgrades are complete, this merge policy will set flag back to false
   * 
   * @param enableUpgrades allow this policy to upgrade segments: Default false
   */
  public void setEnableUpgrades(boolean enableUpgrades) {
    this.enableUpgrades = enableUpgrades;
  }
  
  public boolean getEnableUpgrades() {
    return enableUpgrades;
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
  
  public int getMaxUpgradesAtATime() {
    return maxUpgradesAtATime;
  }
  
  public void setShouldSegmentUpgrade(Predicate<SegmentCommitInfo> shouldSegmentUpgrade) {
    this.shouldSegmentUpgrade = shouldSegmentUpgrade;
  }
  
  /** 
   * Return if a segment is the latest version
   * 
   * @param si some segment commit info
   * @return if that segment is the latest version
   */
  public static boolean segmentIsLatestVersion(SegmentCommitInfo si) {
    return !Version.LATEST.equals(si.info.getVersion());
  }
  
  @Override
  public MergeSpecification findForcedMerges(SegmentInfos segmentInfos, int maxSegmentCount, Map<SegmentCommitInfo,Boolean> segmentsToMerge, IndexWriter writer) throws IOException {
    
    MergeSpecification spec = in.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, writer);
    
    if(enableUpgrades) {
     
      // first find all old segments
      final Map<SegmentCommitInfo,Boolean> segmentsNeedingUpgrade = findSegmentsNeedingUpgrade(segmentInfos, segmentsToMerge);
      
      if (verbose(writer)) {
        message("findForcedMerges: segmentsToUpgrade=" + segmentsNeedingUpgrade, writer);
      }
        
      if (segmentsNeedingUpgrade.isEmpty()) {
        enableUpgrades = false;  // Nothing to upgrade
        return spec;
      }
        

      // Remove segments already being merged from segments needing upgrade.
      if (spec != null) {
        for (final OneMerge om : spec.merges) {
          segmentsNeedingUpgrade.keySet().removeAll(om.segments);
        }
      }
      
      // Add other segments missed by the wrapped merge policy to be upgraded
      return maybeUpdateSpecAndUpgradeProgress(spec, segmentsNeedingUpgrade, segmentInfos, writer);

    }

    return spec;
  }
  
  /**
   * Updates the the merge spec with old segments needing upgrade. Also sets whether or not to the upgrade needs to continue (upgradeInProgress=false)
   * 
   * @param spec the MergeSpecification to update
   * @param segmentsNeedingUpgrade the segments needing upgrade
   * @param segmentInfos all segment infos
   * @param writer the index writer
   * @return the possibly updated MergeSpecification
   */
  private MergeSpecification maybeUpdateSpecAndUpgradeProgress(MergeSpecification spec, Map<SegmentCommitInfo,Boolean> segmentsNeedingUpgrade, SegmentInfos segmentInfos, IndexWriter writer) {
    if (!segmentsNeedingUpgrade.isEmpty()) {
      if (verbose(writer)) {
        message("findForcedMerges: " +  in.getClass().getSimpleName() +
        " does not want to merge all old segments, rewrite remaining ones into upgraded segments: " + segmentsNeedingUpgrade, writer);
      }
      
      if (spec == null) {
        spec = new MergeSpecification();
      }
      
      final int numOriginalMerges = spec.merges.size();
      
      for (SegmentCommitInfo si: segmentInfos) {
        
        if(!segmentsNeedingUpgrade.containsKey(si)) {
          continue;
        }
        
        // Add a merge of only the upgrading segment to the spec
        // We don't want to merge, just upgrade
        spec.add(new OneMerge(Collections.singletonList(si)));
        
        int numMergeWeAdded = spec.merges.size() - numOriginalMerges; 
        if(numMergeWeAdded == maxUpgradesAtATime) {
          return spec;
        }
        
      }
      
      // We found  we have less than the max number but greater than 0
      if(spec.merges.size() > numOriginalMerges) {
          return spec;
      }
      
    }
    
    // Only set this once there are 0 segments needing upgrading, because when we return a
    // spec, IndexWriter may (silently!) reject that merge if some of the segments we asked
    // to be merged were already being (naturally) merged:
    enableUpgrades = false;
    
    return spec;
  }
  
  private Map<SegmentCommitInfo,Boolean> findSegmentsNeedingUpgrade(SegmentInfos segmentInfos, Map<SegmentCommitInfo,Boolean> segmentsToMerge) {
    final Map<SegmentCommitInfo,Boolean> segmentsNeedingUpgrade = new HashMap<>();
    
    for (final SegmentCommitInfo si : segmentInfos) {
      final Boolean v = segmentsToMerge.get(si);
      if (v != null && shouldSegmentUpgrade.test(si)) {
        segmentsNeedingUpgrade.put(si, v);
      }
    }
    
    return segmentsNeedingUpgrade;
  }
  
  private boolean verbose(IndexWriter writer) {
    return writer != null && writer.infoStream.isEnabled("UPGMP");
  }

  private void message(String message, IndexWriter writer) {
    writer.infoStream.message("UPGMP", message);
  }
}
