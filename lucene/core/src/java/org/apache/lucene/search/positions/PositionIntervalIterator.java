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
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * 
 * @lucene.experimental
 */
// nocommit - javadoc
public abstract class PositionIntervalIterator {
  
  public static final PositionIntervalIterator[] EMPTY = new PositionIntervalIterator[0];
  public static final PositionIntervalIterator NO_MORE_POSITIONS = new NoPositionsIntervalIterator();
  public static final int NO_MORE_DOCS = Integer.MAX_VALUE;
  
  protected int currentDoc = -1;
  protected final Scorer scorer;
  protected final boolean collectPositions;
  
  public PositionIntervalIterator(Scorer scorer, boolean collectPositions) {
    this.scorer = scorer;
    this.collectPositions = collectPositions;
  }
  
  public abstract int advanceTo(int docId) throws IOException;
  
  public abstract PositionInterval next() throws IOException;
  
  public abstract void collect(PositionCollector collector);
  
  public abstract PositionIntervalIterator[] subs(boolean inOrder);
  
  
  public abstract int matchDistance();
  
  public int docID() {
    return currentDoc;
  }
  
  public Scorer getScorer() {
    return scorer;
  }
  
  public static interface PositionIntervalFilter {
    public abstract PositionIntervalIterator filter(
        PositionIntervalIterator iter);
  }
  
  public static class PositionInterval implements Cloneable {
    
    public int begin;
    public int end;
    public int offsetBegin;
    public int offsetEnd;
    
    public PositionInterval(int begin, int end, int offsetBegin, int offsetEnd) {
      this.begin = begin;
      this.end = end;
      this.offsetBegin = offsetBegin;
      this.offsetEnd = offsetEnd;
    }
    
    public PositionInterval() {
      this(Integer.MIN_VALUE, Integer.MIN_VALUE, -1, -1);
    }
    
    public boolean lessThanExclusive(PositionInterval other) {
      return begin < other.begin && end < other.end;
    }
    
    public boolean lessThan(PositionInterval other) {
      return begin <= other.begin && end <= other.end;
    }
    
    public boolean greaterThanExclusive(PositionInterval other) {
      return begin > other.begin && end > other.end;
    }
    
    public boolean greaterThan(PositionInterval other) {
      return begin >= other.begin && end >= other.end;
    }
    
    public boolean contains(PositionInterval other) {
      return begin <= other.begin && other.end <= end;
    }
    
    public void copy(PositionInterval other) {
      begin = other.begin;
      end = other.end;
      offsetBegin = other.offsetBegin;
      offsetEnd = other.offsetEnd;
    }
    
    public boolean nextPayload(BytesRef ref) throws IOException {
      return false;
    }
    
    public boolean payloadAvailable() {
      return false;
    }
    
    public void reset() {
      offsetBegin = offsetEnd = -1;
      begin = end = Integer.MIN_VALUE;
    }
    
    @Override
    public Object clone() {
      try {
        return super.clone();
      } catch (CloneNotSupportedException e) {
        throw new RuntimeException(); // should not happen
      }
    }
    
    @Override
    public String toString() {
      return "PositionInterval [begin=" + begin + "(" + offsetBegin + "), end="
          + end + "(" + offsetEnd + ")]";
    }
    
  }
  
  public static interface PositionCollector {
    public void collectLeafPosition(Scorer scorer, PositionInterval interval,
        int docID);
    
    public void collectComposite(Scorer scorer, PositionInterval interval,
        int docID);
    
  }
  
  private static final class NoPositionsIntervalIterator extends
      PositionIntervalIterator {
    
    public NoPositionsIntervalIterator() {
      super(null, false);
    }
    
    @Override
    public int advanceTo(int docId) throws IOException {
      return PositionIntervalIterator.NO_MORE_DOCS;
    }
    
    @Override
    public PositionInterval next() throws IOException {
      return null;
    }
    
    @Override
    public void collect(PositionCollector collectoc) {}
    
    @Override
    public PositionIntervalIterator[] subs(boolean inOrder) {
      return EMPTY;
    }

    @Override
    public int matchDistance() {
      return Integer.MAX_VALUE;
    }
    
  }
  
}
