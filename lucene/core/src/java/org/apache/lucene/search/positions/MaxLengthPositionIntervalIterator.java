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
import java.io.IOException;

import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.positions.IntervalQueue.IntervalRef;

/**
 * An interval iterator that has the semantics of sloppy phrase query.
 */
public class MaxLengthPositionIntervalIterator extends PositionIntervalIterator {
  
  private final int maxLen;
  private int matchDistance;
  private final PositionIntervalIterator iterator;
  
  public MaxLengthPositionIntervalIterator(Scorer scorer, int maxLength,
      boolean collectPositions, PositionIntervalIterator iterator)
      throws IOException {
    super(scorer, collectPositions);
    this.maxLen = maxLength;
    this.iterator = iterator;
  }
  
  @Override
  public PositionInterval next() throws IOException {
    PositionInterval current;
    do {
      if ((current = iterator.next()) != null) {
        matchDistance = current.end - current.begin;
        if (matchDistance <= maxLen) {
//          System.out.println(matchDistance);
          break;
        }
      } else {
        break;
      }
    } while (true);
    return current;
  }
  
  @Override
  public int advanceTo(int docId) throws IOException {
    return currentDoc = iterator.advanceTo(docId);
  }
  
  public int matchLength() {
    return matchDistance;
  }
  
  public static PositionIntervalIterator create(Scorer scorer, boolean collectPositions,
        PositionIntervalIterator iterator, int... offsets) {
    if (offsets.length == 1) {
      return new SingleSlopplyIntervalIterator(scorer, collectPositions, iterator, offsets[0]);
    } else {
      return new SloppyGroupIntervalIterator(scorer, collectPositions, iterator, offsets);
    }
    
  }
  
  private final static class SingleSlopplyIntervalIterator extends
      PositionIntervalIterator {
    private PositionInterval realInterval;
    private final PositionInterval sloppyInterval = new PositionInterval();
    private final PositionIntervalIterator iterator;
    private int offset;
    
    public SingleSlopplyIntervalIterator(Scorer scorer,
        boolean collectPositions, PositionIntervalIterator iterator, int offset) {
      super(scorer, collectPositions);
      this.iterator = iterator;
      this.offset = offset;
    }
    
    @Override
    public int advanceTo(int docId) throws IOException {
      return currentDoc = iterator.advanceTo(docId);
    }
    
    @Override
    public PositionInterval next() throws IOException {
      if ((realInterval = iterator.next()) != null) {
        sloppyInterval.begin = sloppyInterval.end = realInterval.begin - offset;
        sloppyInterval.offsetBegin = realInterval.offsetBegin;
        sloppyInterval.offsetEnd = realInterval.offsetEnd;
        return sloppyInterval;
      }
      return null;
    }
    
    @Override
    public void collect(PositionCollector collector) {
      collector.collectLeafPosition(scorer, realInterval, currentDoc);
      
    }
    
    @Override
    public PositionIntervalIterator[] subs(boolean inOrder) {
      return null;
    }
    
  }
  
  private final static class SloppyGroupIntervalIterator extends
      PositionIntervalIterator {
    
    private final PositionInterval sloppyGroupInterval = new PositionInterval();
    private final int[] offsets;
    private final PositionInterval[] intervalPositions;
    private final PositionIntervalIterator groupIterator;
    private int currentIndex;
    private boolean initialized;
    
    public SloppyGroupIntervalIterator(Scorer scorer, boolean collectPositions,
        PositionIntervalIterator groupIterator, int... offsets) {
      super(scorer, collectPositions);
      this.offsets = offsets;
      this.groupIterator = groupIterator;
      this.intervalPositions = new PositionInterval[offsets.length];
      for (int i = 0; i < intervalPositions.length; i++) {
        intervalPositions[i] = new PositionInterval();
      }
    }
    
    @Override
    public int advanceTo(int docId) throws IOException {
      initialized = false;
      return currentDoc = groupIterator.advanceTo(docId);
    }
    
    @Override
    public PositionInterval next() throws IOException {
      sloppyGroupInterval.begin = Integer.MAX_VALUE;
      sloppyGroupInterval.end = Integer.MIN_VALUE;
      if (!initialized) {
        initialized = true;
        
        currentIndex = 0;
        for (int i = 0; i < offsets.length; i++) {
          PositionInterval current;
          if ((current = groupIterator.next()) != null) {
            intervalPositions[i].copy(current);

            int p = current.begin - offsets[i];
            sloppyGroupInterval.begin = Math.min(sloppyGroupInterval.begin, p);
            sloppyGroupInterval.end = Math.max(sloppyGroupInterval.end, p);
          } else {
            return null;
          }
        }
        sloppyGroupInterval.offsetBegin = intervalPositions[0].offsetBegin;
        sloppyGroupInterval.offsetEnd = intervalPositions[intervalPositions.length-1].offsetEnd;
        return sloppyGroupInterval;
      }
      PositionInterval current;
      if ((current = groupIterator.next()) != null) {
        final int currentFirst = currentIndex++ % intervalPositions.length;
        intervalPositions[currentFirst].copy(current);
        int currentIdx = currentIndex;
        for (int i = 0; i < intervalPositions.length; i++) { // find min / max
          int idx = currentIdx++ % intervalPositions.length;
          int p = intervalPositions[idx].begin - offsets[i];
          sloppyGroupInterval.begin = Math.min(sloppyGroupInterval.begin, p);
          sloppyGroupInterval.end = Math.max(sloppyGroupInterval.end, p);
        }
        sloppyGroupInterval.offsetBegin = intervalPositions[currentIndex % intervalPositions.length].offsetBegin;
        sloppyGroupInterval.offsetEnd = intervalPositions[currentFirst].offsetEnd;
        return sloppyGroupInterval;
      }
      return null;
    }
    
    @Override
    public void collect(PositionCollector collector) {
      int currentIdx = currentIndex+1;
      for (int i = 0; i < intervalPositions.length; i++) { // find min / max
        int idx = currentIdx++ % intervalPositions.length;
        collector.collectLeafPosition(scorer, intervalPositions[idx],
            currentDoc);
      }
      
    }
    
    @Override
    public PositionIntervalIterator[] subs(boolean inOrder) {
      return new PositionIntervalIterator[] {groupIterator};
    }
    
  }
  
  @Override
  public void collect(PositionCollector collector) {
    assert collectPositions;
    this.iterator.collect(collector);
    
  }
  
  @Override
  public PositionIntervalIterator[] subs(boolean inOrder) {
    return null;
  }
}
