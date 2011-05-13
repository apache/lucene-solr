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

import org.apache.lucene.util.Constants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** This {@link MergePolicy} is used for upgrading all existing segments of
  * an index when calling {@link IndexWriter#optimize()}.
  * All other methods delegate to the base {@code MergePolicy} given to the constructor.
  * This allows for an as-cheap-as possible upgrade of an older index by only upgrading segments that
  * are created by previous Lucene versions. Optimize does no longer really optimize
  * it is just used to &quot;optimize&quot; older segment versions away.
  * <p>In general one would use {@link IndexUpgrader}, but for a fully customizeable upgrade,
  * you can use this like any other {@code MergePolicy} and call {@link IndexWriter#optimize()}:
  * <pre class="prettyprint lang-java">
  *  IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_XX, new KeywordAnalyzer());
  *  iwc.setMergePolicy(new UpgradeIndexMergePolicy(iwc.getMergePolicy()));
  *  IndexWriter w = new IndexWriter(dir, iwc);
  *  w.optimize();
  *  w.close();
  * </pre>
  * @lucene.experimental
  * @see IndexUpgrader
  */
public class UpgradeIndexMergePolicy extends MergePolicy {

  protected final MergePolicy base;

  /** Wrap the given {@link MergePolicy} and intercept optimize requests to
   * only upgrade segments written with previous Lucene versions. */
  public UpgradeIndexMergePolicy(MergePolicy base) {
    this.base = base;
  }
  
  /** Returns if the given segment should be upgraded. The default implementation
   * will return {@code !Constants.LUCENE_MAIN_VERSION.equals(si.getVersion())},
   * so all segments created with a different version number than this Lucene version will
   * get upgraded.
   */
  protected boolean shouldUpgradeSegment(SegmentInfo si) {
    return !Constants.LUCENE_MAIN_VERSION.equals(si.getVersion());
  }

  @Override
  public void setIndexWriter(IndexWriter writer) {
    super.setIndexWriter(writer);
    base.setIndexWriter(writer);
  }
  
  @Override
  public MergeSpecification findMerges(SegmentInfos segmentInfos) throws CorruptIndexException, IOException {
    return base.findMerges(segmentInfos);
  }
  
  @Override
  public MergeSpecification findMergesForOptimize(SegmentInfos segmentInfos, int maxSegmentCount, Set<SegmentInfo> segmentsToOptimize) throws CorruptIndexException, IOException {
    // first find all old segments
    final HashSet<SegmentInfo> oldSegments = new HashSet<SegmentInfo>();
    for (final SegmentInfo si : segmentInfos) {
      if (segmentsToOptimize.contains(si) && shouldUpgradeSegment(si)) {
        oldSegments.add(si);
      }
    }
    
    if (verbose()) message("findMergesForOptimize: segmentsToUpgrade=" + oldSegments);
      
    if (oldSegments.isEmpty())
      return null;

    MergeSpecification spec = base.findMergesForOptimize(segmentInfos, maxSegmentCount, oldSegments);    
    
    if (spec != null) {
      // remove all segments that are in merge specification from oldSegments,
      // the resulting set contains all segments that are left over
      // and will be merged to one additional segment:
      for (final OneMerge om : spec.merges) {
        oldSegments.removeAll(om.segments);
      }
    }

    if (!oldSegments.isEmpty()) {
      if (verbose())
        message("findMergesForOptimize: " +  base.getClass().getSimpleName() +
        " does not want to merge all old segments, merge remaining ones into new segment: " + oldSegments);
      final List<SegmentInfo> newInfos = new ArrayList<SegmentInfo>();
      for (final SegmentInfo si : segmentInfos) {
        if (oldSegments.contains(si)) {
          newInfos.add(si);
        }
      }
      // add the final merge
      if (spec == null) {
        spec = new MergeSpecification();
      }
      spec.add(new OneMerge(newInfos));
    }

    return spec;
  }
  
  @Override
  public MergeSpecification findMergesToExpungeDeletes(SegmentInfos segmentInfos) throws CorruptIndexException, IOException {
    return base.findMergesToExpungeDeletes(segmentInfos);
  }
  
  @Override
  public boolean useCompoundFile(SegmentInfos segments, SegmentInfo newSegment) throws IOException {
    return base.useCompoundFile(segments, newSegment);
  }
  
  @Override
  public void close() {
    base.close();
  }
  
  @Override
  public String toString() {
    return "[" + getClass().getSimpleName() + "->" + base + "]";
  }
  
  private boolean verbose() {
    IndexWriter w = writer.get();
    return w != null && w.verbose();
  }

  private void message(String message) {
    if (verbose())
      writer.get().message("UPGMP: " + message);
  }
  
}
