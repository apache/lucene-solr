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
package org.apache.lucene.search.suggest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.Counter;

/**
 * This wrapper buffers incoming elements.
 * @lucene.experimental
 */
public class BufferedInputIterator implements InputIterator {
  // TODO keep this for now
  /** buffered term entries */
  protected BytesRefArray entries = new BytesRefArray(Counter.newCounter());
  /** buffered payload entries */
  protected BytesRefArray payloads = new BytesRefArray(Counter.newCounter());
  /** buffered context set entries */
  protected List<Set<BytesRef>> contextSets = new ArrayList<>();
  /** current buffer position */
  protected int curPos = -1;
  /** buffered weights, parallel with {@link #entries} */
  protected long[] freqs = new long[1];
  private final BytesRefBuilder spare = new BytesRefBuilder();
  private final BytesRefBuilder payloadSpare = new BytesRefBuilder();
  private final boolean hasPayloads;
  private final boolean hasContexts;

  /** Creates a new iterator, buffering entries from the specified iterator */
  public BufferedInputIterator(InputIterator source) throws IOException {
    BytesRef spare;
    int freqIndex = 0;
    hasPayloads = source.hasPayloads();
    hasContexts = source.hasContexts();
    while((spare = source.next()) != null) {
      entries.append(spare);
      if (hasPayloads) {
        payloads.append(source.payload());
      }
      if (hasContexts) {
        contextSets.add(source.contexts());
      }
      if (freqIndex >= freqs.length) {
        freqs = ArrayUtil.grow(freqs, freqs.length+1);
      }
      freqs[freqIndex++] = source.weight();
    }
   
  }

  @Override
  public long weight() {
    return freqs[curPos];
  }

  @Override
  public BytesRef next() throws IOException {
    if (++curPos < entries.size()) {
      entries.get(spare, curPos);
      return spare.get();
    }
    return null;
  }

  @Override
  public BytesRef payload() {
    if (hasPayloads && curPos < payloads.size()) {
      return payloads.get(payloadSpare, curPos);
    }
    return null;
  }

  @Override
  public boolean hasPayloads() {
    return hasPayloads;
  }

  @Override
  public Set<BytesRef> contexts() {
    if (hasContexts && curPos < contextSets.size()) {
      return contextSets.get(curPos);
    }
    return null;
  }

  @Override
  public boolean hasContexts() {
    return hasContexts;
  }
}
