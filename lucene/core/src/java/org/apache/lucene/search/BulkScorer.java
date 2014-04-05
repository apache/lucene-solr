package org.apache.lucene.search;

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

import java.io.IOException;

/** This class is used to score a range of documents at
 *  once, and is returned by {@link Weight#bulkScorer}.  Only
 *  queries that have a more optimized means of scoring
 *  across a range of documents need to override this.
 *  Otherwise, a default implementation is wrapped around
 *  the {@link Scorer} returned by {@link Weight#scorer}. */

public abstract class BulkScorer {

  /** Scores and collects all matching documents.
   * @param collector The collector to which all matching documents are passed.
   */
  public void score(LeafCollector collector) throws IOException {
    score(collector, Integer.MAX_VALUE);
  }

  /**
   * Collects matching documents in a range.
   * 
   * @param collector The collector to which all matching documents are passed.
   * @param max Score up to, but not including, this doc
   * @return true if more matching documents may remain.
   */
  public abstract boolean score(LeafCollector collector, int max) throws IOException;
}
