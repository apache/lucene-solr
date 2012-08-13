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

import java.io.IOException;

/**
 * 
 * @lucene.experimental
 */
// nocommit - javadoc
public abstract class IntervalIterator {
  
  public static final IntervalIterator[] EMPTY = new IntervalIterator[0];
  public static final IntervalIterator NO_MORE_POSITIONS = new EmptyIntervalIterator();
  public static final int NO_MORE_DOCS = Integer.MAX_VALUE;
  
  protected final Scorer scorer;
  protected final boolean collectPositions;
  
  public IntervalIterator(Scorer scorer, boolean collectPositions) {
    this.scorer = scorer;
    this.collectPositions = collectPositions;
  }
  
  public abstract int scorerAdvanced(int docId) throws IOException;
  
  public abstract Interval next() throws IOException;
  
  public abstract void collect(IntervalCollector collector);
  
  public abstract IntervalIterator[] subs(boolean inOrder);
  
  public abstract int matchDistance();
  
  public int docID() {
    return scorer.docID();
  }
  
  public Scorer getScorer() {
    return scorer;
  }
  
  public static interface IntervalFilter {
    public abstract IntervalIterator filter(
        boolean collectPositions, IntervalIterator iter);
  }
  
  public static interface IntervalCollector {
    public void collectLeafPosition(Scorer scorer, Interval interval,
        int docID);
    
    public void collectComposite(Scorer scorer, Interval interval,
        int docID);
    
  }
  
  private static final class EmptyIntervalIterator extends
      IntervalIterator {
    
    public EmptyIntervalIterator() {
      super(null, false);
    }
    
    @Override
    public int scorerAdvanced(int docId) throws IOException {
      return IntervalIterator.NO_MORE_DOCS;
    }
    
    @Override
    public Interval next() throws IOException {
      return null;
    }
    
    @Override
    public void collect(IntervalCollector collectoc) {}
    
    @Override
    public IntervalIterator[] subs(boolean inOrder) {
      return EMPTY;
    }

    @Override
    public int matchDistance() {
      return Integer.MAX_VALUE;
    }
    
  }
  
}
