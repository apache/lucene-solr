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

package org.apache.lucene.concordance.charoffsets;

import java.util.BitSet;

/**
 * Util class used to specify the tokens for which character offsets are requested.
 */


public class TokenCharOffsetRequests {
  private BitSet set = new BitSet();
  private int last = -1;

  /**
   * Is a specific token requested?
   *
   * @param i token number to test
   * @return whether or not this token is requested
   */
  public boolean contains(int i) {
    return set.get(i);
  }

  /**
   * add a request from start to end inclusive
   *
   * @param start range of token offsets to request (inclusive)
   * @param end   end range of token offsets to request (inclusive)
   */
  public void add(int start, int end) {
    for (int i = start; i <= end; i++) {
      add(i);
    }
  }

  /**
   * add a request for a specific token
   *
   * @param i token offset to request the character offsets for
   */
  public void add(int i) {
    set.set(i);
    last = (i > last) ? i : last;
  }

  /**
   * clear the state of this request object for reuse
   */
  public void clear() {
    set.clear();
    last = -1;
  }

  /**
   * @return greatest/last token offset in the request
   */
  public int getLast() {
    return last;
  }

  /**
   * @return the set of tokens whose character offsets are requested
   */
  protected BitSet getSet() {
    return set;
  }
}
