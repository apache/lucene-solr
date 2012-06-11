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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;


/**
 *   Only return those matches that have a specific payload at
 *  the given position.
 *<p/>
 * Do not use this with an SpanQuery that contains a {@link org.apache.lucene.search.spans.SpanNearQuery}.  Instead, use
 * {@link SpanNearPayloadCheckQuery} since it properly handles the fact that payloads
 * aren't ordered by {@link org.apache.lucene.search.spans.SpanNearQuery}.
 *
 **/
public class SpanPayloadCheckQuery extends SpanPositionCheckQuery{
  protected final Collection<byte[]> payloadToMatch;

  /**
   *
   * @param match The underlying {@link org.apache.lucene.search.spans.SpanQuery} to check
   * @param payloadToMatch The {@link java.util.Collection} of payloads to match
   */
  public SpanPayloadCheckQuery(SpanQuery match, Collection<byte[]> payloadToMatch) {
    super(match);
    if (match instanceof SpanNearQuery){
      throw new IllegalArgumentException("SpanNearQuery not allowed");
    }
    this.payloadToMatch = payloadToMatch;
  }

  @Override
  protected AcceptStatus acceptPosition(Spans spans) throws IOException {
    boolean result = spans.isPayloadAvailable();
    if (result == true){
      Collection<byte[]> candidate = spans.getPayload();
      if (candidate.size() == payloadToMatch.size()){
        //TODO: check the byte arrays are the same
        Iterator<byte[]> toMatchIter = payloadToMatch.iterator();
        //check each of the byte arrays, in order
        //hmm, can't rely on order here
        for (byte[] candBytes : candidate) {
          //if one is a mismatch, then return false
          if (Arrays.equals(candBytes, toMatchIter.next()) == false){
            return AcceptStatus.NO;
          }
        }
        //we've verified all the bytes
        return AcceptStatus.YES;
      } else {
        return AcceptStatus.NO;
      }
    }
    return AcceptStatus.YES;
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
  public SpanPayloadCheckQuery clone() {
    SpanPayloadCheckQuery result = new SpanPayloadCheckQuery((SpanQuery) match.clone(), payloadToMatch);
    result.setBoost(getBoost());
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SpanPayloadCheckQuery)) return false;

    SpanPayloadCheckQuery other = (SpanPayloadCheckQuery)o;
    return this.payloadToMatch.equals(other.payloadToMatch)
         && this.match.equals(other.match)
         && this.getBoost() == other.getBoost();
  }

  @Override
  public int hashCode() {
    int h = match.hashCode();
    h ^= (h << 8) | (h >>> 25);  // reversible
    //TODO: is this right?
    h ^= payloadToMatch.hashCode();
    h ^= Float.floatToRawIntBits(getBoost()) ;
    return h;
  }
}