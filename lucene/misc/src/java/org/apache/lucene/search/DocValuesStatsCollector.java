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
package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;

/** A {@link Collector} which computes statistics for a DocValues field. */
public class DocValuesStatsCollector implements Collector {

  private final DocValuesStats<?> stats;

  /** Creates a collector to compute statistics for a DocValues field using the given {@code stats}. */
  public DocValuesStatsCollector(DocValuesStats<?> stats) {
    this.stats = stats;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    boolean shouldProcess = stats.init(context);
    if (!shouldProcess) {
      // Stats cannot be computed for this segment, therefore consider all matching documents as a 'miss'. 
      return new LeafCollector() {
        @Override public void setScorer(Scorer scorer) throws IOException {}

        @Override
        public void collect(int doc) throws IOException {
          // All matching documents in this reader are missing a value
          stats.addMissing();
        }
      };
    }

    return new LeafCollector() {
      @Override public void setScorer(Scorer scorer) throws IOException {}

      @Override
      public void collect(int doc) throws IOException {
        stats.accumulate(doc);
      }
    };
  }

  @Override
  public boolean needsScores() {
    return false;
  }

}
