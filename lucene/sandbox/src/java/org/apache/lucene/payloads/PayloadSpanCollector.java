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
package org.apache.lucene.payloads;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * SpanCollector for collecting payloads
 */
public class PayloadSpanCollector implements SpanCollector {

  private final Collection<byte[]> payloads = new ArrayList<>();

  @Override
  public void collectLeaf(PostingsEnum postings, int position, Term term) throws IOException {
    BytesRef payload = postings.getPayload();
    if (payload == null)
      return;
    final byte[] bytes = new byte[payload.length];
    System.arraycopy(payload.bytes, payload.offset, bytes, 0, payload.length);
    payloads.add(bytes);
  }

  @Override
  public void reset() {
    payloads.clear();
  }

  /**
   * @return the collected payloads
   */
  public Collection<byte[]> getPayloads() {
    return payloads;
  }
}
