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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.lucene.search.positions.PositionIntervalIterator;
import org.apache.lucene.search.positions.PositionIntervalIterator.PositionInterval;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;

public class SpansScorerWrapper extends Spans {
  private final Scorer scorer;
  private final PositionIntervalIterator positions;
  private PositionInterval current;
  private int doc = -1;

  public SpansScorerWrapper(Scorer scorer, PositionIntervalIterator positions) {
    this.scorer = scorer;
    this.positions = positions;
  }

  @Override
  public boolean next() throws IOException {
    if (doc == -1) {
      doc = scorer.nextDoc();
    }

    if (doc == Scorer.NO_MORE_DOCS) {
      return false;
    }

    if ((current = positions.next()) == null) {
      doc = scorer.nextDoc();
      if (doc == Scorer.NO_MORE_DOCS) {
        return false;
      }
      return (current = positions.next()) != null;
    }
    return true;
  }

  @Override
  public boolean skipTo(int target) throws IOException {
    doc = scorer.advance(target);
    if (doc == Scorer.NO_MORE_DOCS) {
      return false;
    }
    return (current = positions.next()) != null;
  }

  @Override
  public int doc() {
    return doc;
  }

  @Override
  public int start() {
    return current.begin;
  }

  @Override
  public int end() {
    return current.end + 1;
  }

  @Override
  public Collection<byte[]> getPayload() throws IOException {
    BytesRef ref = new BytesRef();
    final Collection<byte[]> payloads = new ArrayList<byte[]>();
    while(current.nextPayload(ref)) {
      if (ref.length > 0) {
        byte[] retVal = new byte[ref.length];
        System.arraycopy(ref.bytes, ref.offset, retVal, 0,
            ref.length);
        payloads.add(retVal);
      }
    }
      
    return payloads;
  }

  @Override
  public boolean isPayloadAvailable() {
    return current != null && current.payloadAvailable();
  }

  public PositionInterval current() {
    return current;
  }

}