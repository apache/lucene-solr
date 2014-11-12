package org.apache.lucene.search.highlight.positions;

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

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.posfilter.Interval;
import org.apache.lucene.util.ArrayUtil;

import java.util.Comparator;

/** Used to accumulate position intervals while scoring 
 * @lucene.experimental
 */
public final class DocAndPositions extends ScoreDoc {
  
  public int posCount = 0;
  public Interval[] positions;
  
  public DocAndPositions(int doc) {
    super(doc, 0);
    positions = new Interval[32];
  }
  
  public void storePosition (Interval pos) {
    ensureStorage();
    positions[posCount++] = (Interval) pos.clone();
  }
  
  private void ensureStorage () {
    if (posCount >= positions.length) {
      Interval temp[] = new Interval[positions.length * 2];
      System.arraycopy(positions, 0, temp, 0, positions.length);
      positions = temp;
    }
  }
  
  public Interval[] sortedPositions() {
    ArrayUtil.timSort(positions, 0, posCount, new Comparator<Interval>() {
      public int compare(Interval o1, Interval o2) {
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
