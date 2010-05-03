package org.apache.lucene.analysis.sinks;

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

import org.apache.lucene.util.AttributeSource;

/**
 * Counts the tokens as they go by and saves to the internal list those between the range of lower and upper, exclusive of upper
 *
 **/
public class TokenRangeSinkFilter extends TeeSinkTokenFilter.SinkFilter {
  private int lower;
  private int upper;
  private int count;

  public TokenRangeSinkFilter(int lower, int upper) {
    this.lower = lower;
    this.upper = upper;
  }

  
  @Override
  public boolean accept(AttributeSource source) {
    try {
      if (count >= lower && count < upper){
        return true;
      }
      return false;
    } finally {
      count++;
    }
  }

  @Override
  public void reset() throws IOException {
    count = 0;
  }
}
