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

package org.apache.lucene.queries.intervals;

import org.apache.lucene.search.MatchesIterator;

/**
 * An extension of MatchesIterator that allows the gaps from a wrapped
 * IntervalIterator to be reported.
 *
 * This is necessary because {@link MatchesIterator#getSubMatches()} returns
 * the submatches of all nested matches as a flat iterator, but
 * {@link IntervalIterator#gaps()} only returns the gaps between its immediate
 * sub-matches, so we can't calculate the latter using the former.
 */
interface IntervalMatchesIterator extends MatchesIterator {

  /**
   * The number of top-level gaps inside the current match
   */
  int gaps();

}
