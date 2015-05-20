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

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spans.BufferedSpanCollector;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.SpanCollectorFactory;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * SpanCollector implementation that collects payloads from a {@link Spans}
 */
public class PayloadSpanCollector implements SpanCollector {

  public static final SpanCollectorFactory FACTORY = new SpanCollectorFactory() {
    @Override
    public PayloadSpanCollector newCollector() {
      return new PayloadSpanCollector();
    }
  };

  private final Collection<byte[]> payloads = new ArrayList<>();
  BufferedPayloadCollector bufferedCollector;

  public Collection<byte[]> getPayloads() {
    return payloads;
  }

  @Override
  public void reset() {
    payloads.clear();
  }

  @Override
  public int requiredPostings() {
    return PostingsEnum.PAYLOADS;
  }

  @Override
  public void collectLeaf(PostingsEnum postings, Term term) throws IOException {
    BytesRef payload = postings.getPayload();
    if (payload == null)
      return;
    final byte[] bytes = new byte[payload.length];
    System.arraycopy(payload.bytes, payload.offset, bytes, 0, payload.length);
    payloads.add(bytes);
  }

  @Override
  public BufferedSpanCollector buffer() {
    if (bufferedCollector == null)
      bufferedCollector = new BufferedPayloadCollector();
    bufferedCollector.reset();
    return bufferedCollector;
  }

  @Override
  public SpanCollector bufferedCollector() {
    if (bufferedCollector == null)
      bufferedCollector = new BufferedPayloadCollector();
    return bufferedCollector.candidateCollector;
  }

  class BufferedPayloadCollector implements BufferedSpanCollector {

    final Collection<byte[]> buffer = new ArrayList<>();
    PayloadSpanCollector candidateCollector = new PayloadSpanCollector();

    void reset() {
      buffer.clear();
    }

    @Override
    public void collectCandidate(Spans spans) throws IOException {
      candidateCollector.reset();
      spans.collect(candidateCollector);
    }

    @Override
    public void accept() {
      buffer.addAll(candidateCollector.payloads);
    }

    @Override
    public void replay() {
      payloads.addAll(buffer);
    }
  }
}
