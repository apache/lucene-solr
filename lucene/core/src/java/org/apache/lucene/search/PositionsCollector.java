package org.apache.lucene.search;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.search.posfilter.Interval;

import java.io.IOException;

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

public abstract class PositionsCollector extends SimpleCollector {

  private Scorer scorer;
  private final boolean collectOffsets;

  protected PositionsCollector(boolean collectOffsets) {
    this.collectOffsets = collectOffsets;
  }

  protected PositionsCollector() {
    this(false);
  }

  @Override
  public final void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
  }

  @Override
  public final void collect(int doc) throws IOException {
    while (scorer.nextPosition() != DocsEnum.NO_MORE_POSITIONS) {
      collectPosition(doc, new Interval(scorer));
    }
  }

  protected abstract void collectPosition(int doc, Interval interval);

  @Override
  public int postingFeatures() {
    return collectOffsets ? DocsEnum.FLAG_OFFSETS : DocsEnum.FLAG_POSITIONS;
  }

  @Override
  public final boolean acceptsDocsOutOfOrder() {
    return false;
  }

}
