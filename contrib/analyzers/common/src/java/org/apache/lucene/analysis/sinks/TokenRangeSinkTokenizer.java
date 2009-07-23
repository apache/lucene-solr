package org.apache.lucene.analysis.sinks;

import org.apache.lucene.analysis.SinkTokenizer;
import org.apache.lucene.analysis.Token;

import java.io.IOException;
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


/**
 * Counts the tokens as they go by and saves to the internal list those between the range of lower and upper, exclusive of upper
 *
 **/
public class TokenRangeSinkTokenizer extends SinkTokenizer {
  private int lower;
  private int upper;
  private int count;

  public TokenRangeSinkTokenizer(int lower, int upper) {
    this.lower = lower;
    this.upper = upper;
  }

  public TokenRangeSinkTokenizer(int initCap, int lower, int upper) {
    super(initCap);
    this.lower = lower;
    this.upper = upper;
  }

  public void add(Token t) {
    if (count >= lower && count < upper){
      super.add(t);
    }
    count++;
  }

  public void reset() throws IOException {
    count = 0;
  }
}
