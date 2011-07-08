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
import java.io.Serializable;

import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;

/**
 * 
 * TODO add documentation
 */
@SuppressWarnings("serial")
public abstract class PositionIntervalIterator implements Serializable{

  public static final PositionIntervalIterator[] EMPTY = new PositionIntervalIterator[0];
  protected final Scorer scorer;
  
  public PositionIntervalIterator(Scorer scorer) {
    this.scorer = scorer;
  }

  public abstract PositionInterval next() throws IOException;

  public abstract PositionIntervalIterator[] subs(boolean inOrder);

  public int docID() {
    return scorer.docID();
  }

  public Scorer getScorer() {
    return scorer;
  }

  public static interface PositionIntervalFilter extends Serializable {
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
      begin = end = 0;
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
}
