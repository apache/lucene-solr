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

/**
 * Filters an {@link IntervalIterator}
 */
public interface IntervalFilter {

  /**
   * Filter the passed in IntervalIterator
   * @param collectPositions true if the returned {@link IntervalIterator} will
   *                         be passed to an {@link IntervalCollector}
   * @param iter the {@link IntervalIterator} to filter
   * @return a filtered {@link IntervalIterator}
   */
  public abstract IntervalIterator filter(boolean collectPositions, IntervalIterator iter);

}
