package org.apache.lucene.search.positions;

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

import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;

/**
 * 
 * @lucene.experimental
 */ // nocommit - javadoc
public abstract class PositionIntervalIterator {

  public static final PositionIntervalIterator[] EMPTY = new PositionIntervalIterator[0];
  public static final int NO_MORE_DOCS = Integer.MAX_VALUE;
  public static final PositionCollector EMPTY_COLLECTOR = new PositionCollector() {

    @Override
    public void collectLeafPosition(Scorer scorer, PositionInterval interval,
        int docID) {
    }

    @Override
    public void collectComposite(Scorer scorer, PositionInterval interval,
        int docID) {
    }

  };

  protected int currentDoc = -1;
  protected final Scorer scorer;
  protected PositionCollector collector = EMPTY_COLLECTOR;

  public PositionIntervalIterator(Scorer scorer) {
    this.scorer = scorer;
  }

  public abstract int advanceTo(int docId) throws IOException;

  public abstract PositionInterval next() throws IOException;

  public void setPositionCollector(PositionCollector collector) {
    if (collector == null) {
      throw new IllegalArgumentException("PositionCollector must not be null");
    }
    this.collector = collector;
    PositionIntervalIterator[] subs = subs(false);
    for (PositionIntervalIterator positionIntervalIterator : subs) {
      positionIntervalIterator.setPositionCollector(collector);
    }
  }


  public abstract void collect();

  public abstract PositionIntervalIterator[] subs(boolean inOrder);
  
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

    public PositionInterval(int begin, int end) {
      this.begin = begin;
      this.end = end;
    }

    public PositionInterval() {
      this(0, 0);
    }

    public boolean nextPayload(BytesRef ref) throws IOException {
      return false;
    }

    public boolean payloadAvailable() {
      return false;
    }

    public void reset() {
      begin = end = -1;
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
      return "PositionInterval [begin=" + begin + ", end=" + end + "]";
    }

  }

  public static interface PositionCollector {
    public void collectLeafPosition(Scorer scorer, PositionInterval interval, int docID);
    public void collectComposite(Scorer scorer, PositionInterval interval, int docID);

  }

}
