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

package org.apache.lucene.concordance.util;

import java.util.Map;

/**
 * Simple class to hold document frequencies and term frequencies
 * for terms.
 */
public class SimpleTargetTermResults {
  private final Map<String, Integer> tfs;
  private final Map<String, Integer> dfs;

  /**
   * @param dfs document frequencies
   * @param tfs term frequencies
   */
  protected SimpleTargetTermResults(Map<String, Integer> dfs,
                                    Map<String, Integer> tfs) {
    this.dfs = dfs;
    this.tfs = tfs;
  }

  /**
   * @return term frequency map
   */
  public Map<String, Integer> getTermFreqs() {
    return tfs;
  }

  /**
   * @return document frequency map
   */
  public Map<String, Integer> getDocFreqs() {
    return dfs;
  }
}
