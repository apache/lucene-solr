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
import java.util.Random;
import java.util.Set;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * This wrapper buffers the incoming elements and makes sure they are in
 * random order.
 * @lucene.experimental
 */
public class UnsortedInputIterator extends BufferedInputIterator {
  // TODO keep this for now
  private final int[] ords;
  private int currentOrd = -1;
  private final BytesRefBuilder spare = new BytesRefBuilder();
  private final BytesRefBuilder payloadSpare = new BytesRefBuilder();
  /** 
   * Creates a new iterator, wrapping the specified iterator and
   * returning elements in a random order.
   */
  public UnsortedInputIterator(InputIterator source) throws IOException {
    super(source);
    ords = new int[entries.size()];
    Random random = new Random();
    for (int i = 0; i < ords.length; i++) {
      ords[i] = i;
    }
    for (int i = 0; i < ords.length; i++) {
      int randomPosition = random.nextInt(ords.length);
      int temp = ords[i];
      ords[i] = ords[randomPosition];
      ords[randomPosition] = temp;
    }
  }
  
  @Override
  public long weight() {
    assert currentOrd == ords[curPos];
    return freqs[currentOrd];
  }

  @Override
  public BytesRef next() throws IOException {
    if (++curPos < entries.size()) {
      currentOrd = ords[curPos];
      return entries.get(spare, currentOrd);  
    }
    return null;
  }
  
  @Override
  public BytesRef payload() {
    if (hasPayloads() && curPos < payloads.size()) {
      assert currentOrd == ords[curPos];
      return payloads.get(payloadSpare, currentOrd);
    }
    return null;
  }
  
  @Override
  public Set<BytesRef> contexts() {
    if (hasContexts() && curPos < contextSets.size()) {
      assert currentOrd == ords[curPos];
      return contextSets.get(currentOrd);
    }
    return null;
  }
}
