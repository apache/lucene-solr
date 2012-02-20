package org.apache.lucene.search.suggest;

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
import java.util.Comparator;

import org.apache.lucene.search.spell.SortedIterator;
import org.apache.lucene.search.spell.TermFreqIterator;
import org.apache.lucene.util.BytesRef;

/**
 * This wrapper buffers incoming elements and makes sure they are sorted in
 * ascending lexicographic order.
 */
public class SortedTermFreqIteratorWrapper extends BufferingTermFreqIteratorWrapper implements SortedIterator {

  private final int[] sortedOrds;
  private int currentOrd = -1;
  private final BytesRef spare = new BytesRef();
  private final Comparator<BytesRef> comp;
  

  public SortedTermFreqIteratorWrapper(TermFreqIterator source, Comparator<BytesRef> comp) throws IOException {
    super(source);
    this.sortedOrds = entries.sort(comp);
    this.comp = comp;
  }

  @Override
  public float freq() {
    return freqs[currentOrd];
  }

  @Override
  public BytesRef next() throws IOException {
    if (++curPos < entries.size()) {
      return entries.get(spare, (currentOrd = sortedOrds[curPos]));  
    }
    return null;
  }

  @Override
  public Comparator<BytesRef> comparator() {
    return comp;
  }
  
  
}
