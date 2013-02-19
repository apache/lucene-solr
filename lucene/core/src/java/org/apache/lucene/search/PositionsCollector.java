package org.apache.lucene.search;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.search.posfilter.Interval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

public class PositionsCollector extends Collector {

  private Scorer scorer;
  private int docsSeen = 0;

  private final int numDocs;
  private DocPositions[] positions;

  public PositionsCollector(int numDocs) {
    this.numDocs = numDocs;
    this.positions = new DocPositions[this.numDocs];
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
  }

  @Override
  public void collect(int doc) throws IOException {
    if (docsSeen >= numDocs)
      return;
    DocPositions dp = new DocPositions(doc);
    while (scorer.nextPosition() != DocsEnum.NO_MORE_POSITIONS) {
      dp.positions.add(new Interval(scorer));
    }
    positions[docsSeen] = dp;
    docsSeen++;
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {

  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return true;
  }

  public DocPositions[] getPositions() {
    return positions;
  }

  public int getNumDocs() {
    return docsSeen;
  }

  public static class DocPositions {

    public final int doc;
    public final List<Interval> positions;

    DocPositions(int doc) {
      this.doc = doc;
      this.positions = new ArrayList<Interval>();
    }

  }

}
