package org.apache.lucene.search.spans;
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


import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;


/**
 * Checks to see if the {@link #getMatch()} lies between a start and end position
 *
 * @see org.apache.lucene.search.spans.SpanFirstQuery for a derivation that is optimized for the case where start position is 0
 */
public class SpanPositionRangeQuery extends SpanPositionCheckQuery {
  protected int start = 0;
  protected int end;

  public SpanPositionRangeQuery(SpanQuery match, int start, int end) {
    super(match);
    this.start = start;
    this.end = end;
  }


  @Override
  protected AcceptStatus acceptPosition(Spans spans) throws IOException {
    assert spans.start() != spans.end();
    if (spans.start() >= end)
      return AcceptStatus.NO_AND_ADVANCE;
    else if (spans.start() >= start && spans.end() <= end)
      return AcceptStatus.YES;
    else
      return AcceptStatus.NO;
  }


  /**
   * @return The minimum position permitted in a match
   */
  public int getStart() {
    return start;
  }

  /**
   * @return the maximum end position permitted in a match.
   */
  public int getEnd() {
    return end;
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("spanPosRange(");
    buffer.append(match.toString(field));
    buffer.append(", ").append(start).append(", ");
    buffer.append(end);
    buffer.append(")");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  @Override
  public SpanPositionRangeQuery clone() {
    SpanPositionRangeQuery result = new SpanPositionRangeQuery((SpanQuery) match.clone(), start, end);
    result.setBoost(getBoost());
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SpanPositionRangeQuery)) return false;

    SpanPositionRangeQuery other = (SpanPositionRangeQuery)o;
    return this.end == other.end && this.start == other.start
         && this.match.equals(other.match)
         && this.getBoost() == other.getBoost();
  }

  @Override
  public int hashCode() {
    int h = match.hashCode();
    h ^= (h << 8) | (h >>> 25);  // reversible
    h ^= Float.floatToRawIntBits(getBoost()) ^ end ^ start;
    return h;
  }

}