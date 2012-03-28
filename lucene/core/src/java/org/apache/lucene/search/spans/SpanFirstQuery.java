package org.apache.lucene.search.spans;

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

import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;

/** Matches spans near the beginning of a field.
 * <p/> 
 * This class is a simple extension of {@link SpanPositionRangeQuery} in that it assumes the
 * start to be zero and only checks the end boundary.
 *
 *
 *  */
public class SpanFirstQuery extends SpanPositionRangeQuery {

  /** Construct a SpanFirstQuery matching spans in <code>match</code> whose end
   * position is less than or equal to <code>end</code>. */
  public SpanFirstQuery(SpanQuery match, int end) {
    super(match, 0, end);
  }

  @Override
  protected AcceptStatus acceptPosition(Spans spans) throws IOException {
    assert spans.start() != spans.end() : "start equals end: " + spans.start();
    if (spans.start() >= end)
      return AcceptStatus.NO_AND_ADVANCE;
    else if (spans.end() <= end)
      return AcceptStatus.YES;
    else
      return AcceptStatus.NO;
  }


  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("spanFirst(");
    buffer.append(match.toString(field));
    buffer.append(", ");
    buffer.append(end);
    buffer.append(")");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  @Override
  public SpanFirstQuery clone() {
    SpanFirstQuery spanFirstQuery = new SpanFirstQuery((SpanQuery) match.clone(), end);
    spanFirstQuery.setBoost(getBoost());
    return spanFirstQuery;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SpanFirstQuery)) return false;

    SpanFirstQuery other = (SpanFirstQuery)o;
    return this.end == other.end
         && this.match.equals(other.match)
         && this.getBoost() == other.getBoost();
  }

  @Override
  public int hashCode() {
    int h = match.hashCode();
    h ^= (h << 8) | (h >>> 25);  // reversible
    h ^= Float.floatToRawIntBits(getBoost()) ^ end;
    return h;
  }


}
