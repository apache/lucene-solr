package org.apache.lucene.search.highlight;


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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Lightweight class to hold term, weight, and positions used for scoring this
 * term.
 */
public class WeightedSpanTerm extends WeightedTerm{
  boolean positionSensitive;
  private List<PositionSpan> positionSpans = new ArrayList<PositionSpan>();

  /**
   * @param weight
   * @param term
   */
  public WeightedSpanTerm(float weight, String term) {
    super(weight, term);
    this.positionSpans = new ArrayList<PositionSpan>();
  }

  /**
   * @param weight
   * @param term
   * @param positionSensitive
   */
  public WeightedSpanTerm(float weight, String term, boolean positionSensitive) {
    super(weight, term);
    this.positionSensitive = positionSensitive;
  }

  /**
   * Checks to see if this term is valid at <code>position</code>.
   *
   * @param position
   *            to check against valid term positions
   * @return true iff this term is a hit at this position
   */
  public boolean checkPosition(int position) {
    // There would probably be a slight speed improvement if PositionSpans
    // where kept in some sort of priority queue - that way this method
    // could
    // bail early without checking each PositionSpan.
    Iterator<PositionSpan> positionSpanIt = positionSpans.iterator();

    while (positionSpanIt.hasNext()) {
      PositionSpan posSpan = positionSpanIt.next();

      if (((position >= posSpan.start) && (position <= posSpan.end))) {
        return true;
      }
    }

    return false;
  }

  public void addPositionSpans(List<PositionSpan> positionSpans) {
    this.positionSpans.addAll(positionSpans);
  }

  public boolean isPositionSensitive() {
    return positionSensitive;
  }

  public void setPositionSensitive(boolean positionSensitive) {
    this.positionSensitive = positionSensitive;
  }

  public List<PositionSpan> getPositionSpans() {
    return positionSpans;
  }

}


