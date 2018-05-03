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

import java.util.List;

/**
 * Information about upcoming impacts, ie. (freq, norm) pairs.
 */
public abstract class Impacts {

  /** Sole constructor. Typically invoked by sub classes. */
  protected Impacts() {}

  /**
   * Return the number of levels on which we have impacts.
   * The returned value is always greater than 0 and may not always be the
   * same, even on a single postings list, depending on the current doc ID.
   */
  public abstract int numLevels();

  /**
   * Return the maximum inclusive doc ID until which the list of impacts
   * returned by {@link #getImpacts(int)} is valid. This is a non-decreasing
   * function of {@code level}.
   */
  public abstract int getDocIdUpTo(int level);

  /**
   * Return impacts on the given level. These impacts are sorted by increasing
   * frequency and increasing unsigned norm, and only valid until the doc ID
   * returned by {@link #getDocIdUpTo(int)} for the same level, included.
   * The returned list is never empty.
   * NOTE: There is no guarantee that these impacts actually appear in postings,
   * only that they trigger scores that are greater than or equal to the impacts
   * that actually appear in postings.
   */
  public abstract List<Impact> getImpacts(int level);

}
