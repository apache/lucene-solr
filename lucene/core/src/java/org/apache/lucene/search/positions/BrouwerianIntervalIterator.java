package org.apache.lucene.search.positions;

import java.io.IOException;

import org.apache.lucene.search.Scorer;

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

public class BrouwerianIntervalIterator extends IntervalIterator {
  
  private final IntervalIterator minuted;
  private final IntervalIterator subtracted;
  private Interval subtractedInterval = new Interval();
  private Interval currentInterval = new Interval();

  public BrouwerianIntervalIterator(Scorer scorer, boolean collectIntervals, IntervalIterator minuted, IntervalIterator subtracted) {
    super(scorer, collectIntervals);
    this.minuted = minuted;
    this.subtracted = subtracted;
  }
  

  @Override
  public int scorerAdvanced(int docId) throws IOException {
    subtractedInterval.reset();
    minuted.scorerAdvanced(docId);
    subtracted.scorerAdvanced(docId);
    return docId;
  }
  
  @Override
  public Interval next() throws IOException {
    if (subtracted.docID() != minuted.docID()) {
      return currentInterval = minuted.next();
    }
    while ((currentInterval = minuted.next()) != null) {
      while(subtractedInterval.lessThan(currentInterval) && (subtractedInterval = subtracted.next()) != null) {
      }
      if (subtractedInterval == null || subtractedInterval.greaterThan(currentInterval)) {
        return currentInterval;
      }
    }
    return currentInterval;
  }
  
  @Override
  public void collect(IntervalCollector collector) {
    assert collectIntervals;
    collector.collectComposite(scorer, currentInterval, docID());
    minuted.collect(collector);
    
  }
  
  @Override
  public IntervalIterator[] subs(boolean inOrder) {
    return new IntervalIterator[] {minuted, subtracted};
  }


  @Override
  public int matchDistance() {
    return minuted.matchDistance();
  }
  
}
