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
package org.apache.lucene.queries.payloads;
import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ToStringUtils;


/**
 * Only return those matches that have a specific payload at
 * the given position.
 *
 * @deprecated Use {@link SpanPayloadCheckQuery}
 */
@Deprecated
public class SpanNearPayloadCheckQuery extends SpanPayloadCheckQuery {

  /**
   * @param match          The underlying {@link org.apache.lucene.search.spans.SpanQuery} to check
   * @param payloadToMatch The {@link java.util.List} of payloads to match
   */
  public SpanNearPayloadCheckQuery(SpanNearQuery match, List<BytesRef> payloadToMatch) {
    super(match, payloadToMatch);
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("spanPayCheck(");
    buffer.append(match.toString(field));
    buffer.append(", payloadRef: ");
    for (BytesRef bytes : payloadToMatch) {
      buffer.append(Term.toString(bytes));
      buffer.append(';');
    }
    buffer.append(")");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  @Override
  public SpanNearPayloadCheckQuery clone() {
    SpanNearPayloadCheckQuery result = new SpanNearPayloadCheckQuery((SpanNearQuery) match.clone(), payloadToMatch);
    result.setBoost(getBoost());
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (! super.equals(o)) {
      return false;
    }
    SpanNearPayloadCheckQuery other = (SpanNearPayloadCheckQuery) o;
    return this.payloadToMatch.equals(other.payloadToMatch);
  }

  @Override
  public int hashCode() {
    int h = super.hashCode();
    h = (h * 15) ^ payloadToMatch.hashCode();
    return h;
  }
}