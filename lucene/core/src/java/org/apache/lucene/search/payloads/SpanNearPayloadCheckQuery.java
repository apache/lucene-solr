package org.apache.lucene.search.payloads;
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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.FilterSpans.AcceptStatus;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanPositionCheckQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;


/**
 * Only return those matches that have a specific payload at
 * the given position.
 */
public class SpanNearPayloadCheckQuery extends SpanPositionCheckQuery {

  protected final Collection<byte[]> payloadToMatch;

  /**
   * @param match          The underlying {@link org.apache.lucene.search.spans.SpanQuery} to check
   * @param payloadToMatch The {@link java.util.Collection} of payloads to match
   */
  public SpanNearPayloadCheckQuery(SpanNearQuery match, Collection<byte[]> payloadToMatch) {
    super(match);
    this.payloadToMatch = Objects.requireNonNull(payloadToMatch);
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return createWeight(searcher, needsScores, PayloadSpanCollector.FACTORY);
  }

  @Override
  protected AcceptStatus acceptPosition(Spans spans, SpanCollector collector) throws IOException {

    PayloadSpanCollector payloadCollector = (PayloadSpanCollector) collector;

    payloadCollector.reset();
    spans.collect(payloadCollector);

    Collection<byte[]> candidate = payloadCollector.getPayloads();
    if (candidate.size() == payloadToMatch.size()) {
      //TODO: check the byte arrays are the same
      //hmm, can't rely on order here
      int matches = 0;
      for (byte[] candBytes : candidate) {
        //Unfortunately, we can't rely on order, so we need to compare all
        for (byte[] payBytes : payloadToMatch) {
          if (Arrays.equals(candBytes, payBytes) == true) {
            matches++;
            break;
          }
        }
      }
      if (matches == payloadToMatch.size()){
        //we've verified all the bytes
        return AcceptStatus.YES;
      } else {
        return AcceptStatus.NO;
      }
    } else {
      return AcceptStatus.NO;
    }

  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("spanPayCheck(");
    buffer.append(match.toString(field));
    buffer.append(", payloadRef: ");
    for (byte[] bytes : payloadToMatch) {
      ToStringUtils.byteArray(buffer, bytes);
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