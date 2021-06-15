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

package org.apache.lucene.sandbox.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;

/**
 * This class wraps a Collector and times the execution of: - setScorer() - collect() -
 * doSetNextReader() - needsScores()
 *
 * <p>QueryProfiler facilitates the linking of the Collector graph
 */
public class ProfilerCollector implements Collector {

  /** A more friendly representation of the Collector's class name */
  private final String collectorName;

  /** A "hint" to help provide some context about this Collector */
  private final String reason;

  /** The wrapped collector */
  private final ProfilerCollectorWrapper collector;

  /** A list of "embedded" children collectors */
  private final List<ProfilerCollector> children;

  public ProfilerCollector(Collector collector, String reason, List<ProfilerCollector> children) {
    this.collector = new ProfilerCollectorWrapper(collector);
    this.reason = reason;
    this.collectorName = deriveCollectorName(collector);
    this.children = children;
  }

  /** @return the profiled time for this collector (inclusive of children) */
  public long getTime() {
    return collector.getTime();
  }

  /** @return a human readable "hint" about what this collector was used for */
  public String getReason() {
    return this.reason;
  }

  /** @return the lucene class name of the collector */
  public String getName() {
    return this.collectorName;
  }

  /**
   * Creates a human-friendly representation of the Collector name.
   *
   * @param c The Collector to derive a name from
   * @return A (hopefully) prettier name
   */
  private String deriveCollectorName(Collector c) {
    return c.getClass().getSimpleName();
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    return collector.getLeafCollector(context);
  }

  @Override
  public ScoreMode scoreMode() {
    return collector.scoreMode();
  }

  public ProfilerCollectorResult getProfileResult() {
    return ProfilerCollector.doGetCollectorTree(this);
  }

  private static ProfilerCollectorResult doGetCollectorTree(ProfilerCollector collector) {
    List<ProfilerCollectorResult> childResults = new ArrayList<>(collector.children.size());
    for (ProfilerCollector child : collector.children) {
      ProfilerCollectorResult result = doGetCollectorTree(child);
      childResults.add(result);
    }
    return new ProfilerCollectorResult(
        collector.getName(), collector.getReason(), collector.getTime(), childResults);
  }
}
