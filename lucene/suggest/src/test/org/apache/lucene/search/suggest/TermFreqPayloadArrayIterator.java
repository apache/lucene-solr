package org.apache.lucene.search.suggest;

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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.lucene.search.spell.TermFreqIterator;
import org.apache.lucene.search.spell.TermFreqPayloadIterator;
import org.apache.lucene.util.BytesRef;

/**
 * A {@link TermFreqIterator} over a sequence of {@link TermFreq}s.
 */
public final class TermFreqPayloadArrayIterator implements TermFreqPayloadIterator {
  private final Iterator<TermFreqPayload> i;
  private TermFreqPayload current;
  private final BytesRef spare = new BytesRef();

  public TermFreqPayloadArrayIterator(Iterator<TermFreqPayload> i) {
    this.i = i;
  }

  public TermFreqPayloadArrayIterator(TermFreqPayload[] i) {
    this(Arrays.asList(i));
  }

  public TermFreqPayloadArrayIterator(Iterable<TermFreqPayload> i) {
    this(i.iterator());
  }
  
  @Override
  public long weight() {
    return current.v;
  }

  @Override
  public BytesRef next() {
    if (i.hasNext()) {
      current = i.next();
      spare.copyBytes(current.term);
      return spare;
    }
    return null;
  }

  @Override
  public BytesRef payload() {
    return current.payload;
  }

  @Override
  public Comparator<BytesRef> getComparator() {
    return null;
  }
}