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

package org.apache.solr.spelling.suggest;


import java.util.ArrayList;
import java.util.List;

import org.apache.solr.util.TermFreqIterator;

/**
 * This wrapper buffers incoming elements.
 */
public class BufferingTermFreqIteratorWrapper implements TermFreqIterator {

  /** Entry in the buffer. */
  public static final class Entry implements Comparable<Entry> {
    String word;
    float freq;
    
    public Entry(String word, float freq) {
      this.word = word;
      this.freq = freq;
    }
    
    @Override
    public int compareTo(Entry o) {
      return word.compareTo(o.word);
    }    
  }

  protected ArrayList<Entry> entries = new ArrayList<Entry>();
  
  protected int curPos;
  protected Entry curEntry;
  
  public BufferingTermFreqIteratorWrapper(TermFreqIterator source) {
    // read all source data into buffer
    while (source.hasNext()) {
      String w = source.next();
      Entry e = new Entry(w, source.freq());
      entries.add(e);
    }
    curPos = 0;
  }

  @Override
  public float freq() {
    return curEntry.freq;
  }

  @Override
  public boolean hasNext() {
    return curPos < entries.size();
  }

  @Override
  public String next() {
    curEntry = entries.get(curPos);
    curPos++;
    return curEntry.word;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove is not supported");
  }
  
  public List<Entry> entries() {
    return entries;
  }
}
