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
 * An interval iterator that has the semantics of sloppy phrase query.
 */
public class SloppyIntervalIterator extends IntervalIterator {
  private final int maxLen;
  private int matchDistance;
  private final IntervalIterator iterator;

  /**
   * Create a SloppyIntervalIterator that matches subiterators within
   * a specified maxLength
   * @param scorer the parent Scorer
   * @param maxLength the maximum distance between the first and last subiterator match
   * @param collectIntervals true if intervals will be collected
   * @param iterators the subiterators
   * @throws IOException if an low level I/O exception occurs
   */
  public SloppyIntervalIterator(Scorer scorer, int maxLength,
      boolean collectIntervals, IntervalIterator... iterators)
      throws IOException {
    super(scorer, collectIntervals);
    this.maxLen = maxLength;
    this.iterator = new ConjunctionIntervalIterator(scorer, collectIntervals, iterators);
  }
  
  @Override
  public Interval next() throws IOException {
    Interval current;
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
  public int scorerAdvanced(int docId) throws IOException {
    return iterator.scorerAdvanced(docId);
  }
  
  public int matchDistance() {
    return matchDistance;
  }
  
  public static IntervalIterator create(Scorer scorer, boolean collectIntervals,
        IntervalIterator iterator, int... offsets) {
    if (offsets.length == 1) {
      return new SingleSlopplyIntervalIterator(scorer, collectIntervals, iterator, offsets[0]);
    } else {
      return new SloppyGroupIntervalIterator(scorer, collectIntervals, iterator, offsets);
    }
    
  }
  
  private final static class SingleSlopplyIntervalIterator extends
      IntervalIterator {
    private Interval realInterval;
    private final Interval sloppyInterval = new Interval();
    private final IntervalIterator iterator;
    private int offset;
    
    public SingleSlopplyIntervalIterator(Scorer scorer,
        boolean collectIntervals, IntervalIterator iterator, int offset) {
      super(scorer, collectIntervals);
      this.iterator = iterator;
      this.offset = offset;
    }
    
    @Override
    public int scorerAdvanced(int docId) throws IOException {
      return iterator.scorerAdvanced(docId);
    }
    
    @Override
    public Interval next() throws IOException {
      if ((realInterval = iterator.next()) != null) {
        sloppyInterval.begin = sloppyInterval.end = realInterval.begin - offset;
        sloppyInterval.offsetBegin = realInterval.offsetBegin;
        sloppyInterval.offsetEnd = realInterval.offsetEnd;
        return sloppyInterval;
      }
      return null;
    }
    
    @Override
    public void collect(IntervalCollector collector) {
      collector.collectLeafPosition(scorer, realInterval, docID());
      
    }
    
    @Override
    public IntervalIterator[] subs(boolean inOrder) {
      return null;
    }

    @Override
    public int matchDistance() {
      return sloppyInterval.end - sloppyInterval.begin;
    }
    
  }
  
  private final static class SloppyGroupIntervalIterator extends
      IntervalIterator {
    
    private final Interval sloppyGroupInterval = new Interval();
    private final int[] offsets;
    private final Interval[] intervalPositions;
    private final IntervalIterator groupIterator;
    private int currentIndex;
    private boolean initialized;
    
    public SloppyGroupIntervalIterator(Scorer scorer, boolean collectIntervals,
        IntervalIterator groupIterator, int... offsets) {
      super(scorer, collectIntervals);
      this.offsets = offsets;
      this.groupIterator = groupIterator;
      this.intervalPositions = new Interval[offsets.length];
      for (int i = 0; i < intervalPositions.length; i++) {
        intervalPositions[i] = new Interval();
      }
    }
    
    @Override
    public int scorerAdvanced(int docId) throws IOException {
      initialized = false;
      return groupIterator.scorerAdvanced(docId);
    }
    
    @Override
    public Interval next() throws IOException {
      sloppyGroupInterval.begin = Integer.MAX_VALUE;
      sloppyGroupInterval.end = Integer.MIN_VALUE;
      if (!initialized) {
        initialized = true;
        
        currentIndex = 0;
        for (int i = 0; i < offsets.length; i++) {
          Interval current;
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
      Interval current;
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
    public void collect(IntervalCollector collector) {
      int currentIdx = currentIndex+1;
      for (int i = 0; i < intervalPositions.length; i++) { // find min / max
        int idx = currentIdx++ % intervalPositions.length;
        collector.collectLeafPosition(scorer, intervalPositions[idx],
            docID());
      }
      
    }
    
    @Override
    public IntervalIterator[] subs(boolean inOrder) {
      return new IntervalIterator[] {groupIterator};
    }

    @Override
    public int matchDistance() {
      return sloppyGroupInterval.end - sloppyGroupInterval.begin;
    }
    
  }
  
  @Override
  public void collect(IntervalCollector collector) {
    assert collectIntervals;
    this.iterator.collect(collector);
    
  }
  
  @Override
  public IntervalIterator[] subs(boolean inOrder) {
    return null;
  }
}
