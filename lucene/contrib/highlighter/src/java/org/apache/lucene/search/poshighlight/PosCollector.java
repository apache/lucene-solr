package org.apache.lucene.search.poshighlight;
/**
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

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.positions.PositionIntervalIterator;
import org.apache.lucene.search.positions.PositionIntervalIterator.PositionCollector;
import org.apache.lucene.search.positions.PositionIntervalIterator.PositionInterval;

/**
 * Collects the first maxDocs docs and their positions matching the query
 * 
 * @lucene.experimental
 */

public class PosCollector extends Collector implements PositionCollector {
  
  int count;
  ScorePosDoc docs[];
  
  public PosCollector (int maxDocs) {
    docs = new ScorePosDoc[maxDocs];
  }
  
  protected Scorer scorer;
  private PositionIntervalIterator positions;

  @Override
  public void collect(int doc) throws IOException {
    if (count >= docs.length)
      return;
    addDoc (doc);
    // consume any remaining positions the scorer didn't report
    docs[count-1].score=scorer.score();
    positions.advanceTo(doc);
    while(positions.next() != null) {
      positions.collect();
    }    
  }
  
  private boolean addDoc (int doc) {
    if (count <= 0 || docs[count-1].doc != doc) {
      ScorePosDoc spdoc = new ScorePosDoc (doc);
      docs[count++] = spdoc;
      return true;
    }
    return false;
  }
  
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }

  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
    positions = scorer.positions();
    positions.setPositionCollector(this);
    // If we want to visit the other scorers, we can, here...
  }
  
  public Scorer getScorer () {
    return scorer;
  }
  
  public ScorePosDoc[] getDocs () {
    ScorePosDoc ret[] = new ScorePosDoc[count];
    System.arraycopy(docs, 0, ret, 0, count);
    return ret;
  }

  public void setNextReader(AtomicReaderContext context) throws IOException {
  }
  
  @Override
  public boolean needsPositions() { return true; }

  @Override
  public void collectLeafPosition(Scorer scorer, PositionInterval interval,
      int docID) {
    addDoc(docID);      
    docs[count - 1].storePosition(interval);
  }

  @Override
  public void collectComposite(Scorer scorer, PositionInterval interval,
      int docID) {
  }

}
