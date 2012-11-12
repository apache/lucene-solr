package org.apache.lucene.search.intervals;

/**
 * Copyright (c) 2012 Lemur Consulting Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class BlockIntervalFilter implements IntervalFilter {

  private final boolean collectLeaves;

  public BlockIntervalFilter() {
    this(true);
  }

  public BlockIntervalFilter(boolean collectLeaves) {
    this.collectLeaves = collectLeaves;
  }

  @Override
  public IntervalIterator filter(boolean collectIntervals, IntervalIterator iter) {
    return new BlockIntervalIterator(collectIntervals, collectLeaves, iter);
  }

}
