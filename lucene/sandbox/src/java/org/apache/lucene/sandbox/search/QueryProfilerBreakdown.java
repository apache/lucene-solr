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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A record of timings for the various operations that may happen during query execution. A node's
 * time may be composed of several internal attributes (rewriting, weighting, scoring, etc).
 */
class QueryProfilerBreakdown {

  /** The accumulated timings for this query node */
  private final QueryProfilerTimer[] timers;

  /** Sole constructor. */
  public QueryProfilerBreakdown() {
    timers = new QueryProfilerTimer[QueryProfilerTimingType.values().length];
    for (int i = 0; i < timers.length; ++i) {
      timers[i] = new QueryProfilerTimer();
    }
  }

  public QueryProfilerTimer getTimer(QueryProfilerTimingType type) {
    return timers[type.ordinal()];
  }

  /** Build a timing count breakdown. */
  public final Map<String, Long> toBreakdownMap() {
    Map<String, Long> map = new HashMap<>(timers.length * 2);
    for (QueryProfilerTimingType type : QueryProfilerTimingType.values()) {
      map.put(type.toString(), timers[type.ordinal()].getApproximateTiming());
      map.put(type.toString() + "_count", timers[type.ordinal()].getCount());
    }
    return Collections.unmodifiableMap(map);
  }

  public final long toTotalTime() {
    long total = 0;
    for (QueryProfilerTimer timer : timers) {
      total += timer.getApproximateTiming();
    }
    return total;
  }
}
