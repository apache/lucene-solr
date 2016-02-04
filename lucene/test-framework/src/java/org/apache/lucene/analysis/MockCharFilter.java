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
package org.apache.lucene.analysis;

import java.io.IOException;
import java.io.Reader;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/** the purpose of this charfilter is to send offsets out of bounds
  if the analyzer doesn't use correctOffset or does incorrect offset math. */
public class MockCharFilter extends CharFilter {
  final int remainder;
  
  // for testing only
  public MockCharFilter(Reader in, int remainder) {
    super(in);
    // TODO: instead of fixed remainder... maybe a fixed
    // random seed?
    this.remainder = remainder;
    if (remainder < 0 || remainder >= 10) {
      throw new IllegalArgumentException("invalid remainder parameter (must be 0..10): " + remainder);
    }
  }
  
  // for testing only, uses a remainder of 0
  public MockCharFilter(Reader in) {
    this(in, 0);
  }
  
  int currentOffset = -1;
  int delta = 0;
  int bufferedCh = -1;
  
  @Override
  public int read() throws IOException {
    // we have a buffered character, add an offset correction and return it
    if (bufferedCh >= 0) {
      int ch = bufferedCh;
      bufferedCh = -1;
      currentOffset++;
      
      addOffCorrectMap(currentOffset, delta-1);
      delta--;
      return ch;
    }
    
    // otherwise actually read one    
    int ch = input.read();
    if (ch < 0)
      return ch;
    
    currentOffset++;
    if ((ch % 10) != remainder || Character.isHighSurrogate((char)ch) || Character.isLowSurrogate((char)ch)) {
      return ch;
    }
    
    // we will double this character, so buffer it.
    bufferedCh = ch;
    return ch;
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    int numRead = 0;
    for (int i = off; i < off + len; i++) {
      int c = read();
      if (c == -1) break;
      cbuf[i] = (char) c;
      numRead++;
    }
    return numRead == 0 ? -1 : numRead;
  }

  @Override
  public int correct(int currentOff) {
    Map.Entry<Integer,Integer> lastEntry = corrections.lowerEntry(currentOff+1);
    int ret = lastEntry == null ? currentOff : currentOff + lastEntry.getValue();
    assert ret >= 0 : "currentOff=" + currentOff + ",diff=" + (ret-currentOff);
    return ret;
  }
  
  protected void addOffCorrectMap(int off, int cumulativeDiff) {
    corrections.put(off, cumulativeDiff);
  }
  
  TreeMap<Integer,Integer> corrections = new TreeMap<>();
}
