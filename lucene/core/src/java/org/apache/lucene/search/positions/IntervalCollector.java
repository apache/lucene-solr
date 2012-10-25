package org.apache.lucene.search.positions;

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

import org.apache.lucene.search.Scorer;

/**
 * Used for collecting matching {@link Interval}s from a search
 */
public interface IntervalCollector {

  /**
   * Collects an individual term match
   * @param scorer the parent scorer
   * @param interval the interval to collect
   * @param docID the docID of the document matched
   */
  public void collectLeafPosition(Scorer scorer, Interval interval, int docID);

  /**
   * Collects a composite interval that may have sub-intervals
   * @param scorer the parent scorer
   * @param interval the interval to collect
   * @param docID the docID of the document matched
   */
  public void collectComposite(Scorer scorer, Interval interval, int docID);

}
