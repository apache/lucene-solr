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

import java.util.Comparator;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.positions.PositionIntervalIterator.PositionInterval;
import org.apache.lucene.util.ArrayUtil;

/** Used to accumulate position intervals while scoring 
 * @lucene.experimental
 */
public class ScorePosDoc extends ScoreDoc {
  
  public int posCount = 0;
  public PositionInterval[] positions;
  
  public ScorePosDoc(int doc) {
    super(doc, 0);
    positions = new PositionInterval[32];
  }
  
  public void storePosition (PositionInterval pos) {
    ensureStorage();
    positions[posCount++] = (PositionInterval) pos.clone();
  }
  
  private void ensureStorage () {
    if (posCount >= positions.length) {
      PositionInterval temp[] = new PositionInterval[positions.length * 2];
      System.arraycopy(positions, 0, temp, 0, positions.length);
      positions = temp;
    }
  }
  
  public PositionInterval[] sortedPositions() {
    ArrayUtil.mergeSort(positions, 0, posCount, new Comparator<PositionInterval>() {
      public int compare(PositionInterval o1, PositionInterval o2) {
        return 
          o1.begin < o2.begin ? -1 : 
            (o1.begin > o2.begin ? 1 :
              (o1.end < o2.end ? -1 : 
                (o1.end > o2.end ? 1 : 
                  0)));
      }
      
    });
    return positions;
  }
}
