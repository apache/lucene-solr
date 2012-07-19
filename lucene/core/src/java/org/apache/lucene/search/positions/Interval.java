package org.apache.lucene.search.positions;
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

import java.io.IOException;

import org.apache.lucene.util.BytesRef;


public class Interval implements Cloneable {
  
  public int begin;
  public int end;
  public int offsetBegin;
  public int offsetEnd;
  
  public Interval(int begin, int end, int offsetBegin, int offsetEnd) {
    this.begin = begin;
    this.end = end;
    this.offsetBegin = offsetBegin;
    this.offsetEnd = offsetEnd;
  }
  
  public Interval() {
    this(Integer.MIN_VALUE, Integer.MIN_VALUE, -1, -1);
  }
  
  public boolean lessThanExclusive(Interval other) {
    return begin < other.begin && end < other.end;
  }
  
  public boolean lessThan(Interval other) {
    return begin <= other.begin && end <= other.end;
  }
  
  public boolean greaterThanExclusive(Interval other) {
    return begin > other.begin && end > other.end;
  }
  
  public boolean greaterThan(Interval other) {
    return begin >= other.begin && end >= other.end;
  }
  
  public boolean contains(Interval other) {
    return begin <= other.begin && other.end <= end;
  }
  
  public void copy(Interval other) {
    begin = other.begin;
    end = other.end;
    offsetBegin = other.offsetBegin;
    offsetEnd = other.offsetEnd;
  }
  
  public boolean nextPayload(BytesRef ref) throws IOException {
    return false;
  }
  
  public boolean payloadAvailable() {
    return false;
  }
  
  public void reset() {
    offsetBegin = offsetEnd = -1;
    begin = end = Integer.MIN_VALUE;
  }
  
  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(); // should not happen
    }
  }
  
  @Override
  public String toString() {
    return "Interval [begin=" + begin + "(" + offsetBegin + "), end="
        + end + "(" + offsetEnd + ")]";
  }
  
}