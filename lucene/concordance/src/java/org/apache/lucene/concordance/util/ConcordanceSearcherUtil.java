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

import java.util.List;

import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.concordance.charoffsets.TokenCharOffsetRequests;

/**
 * In other applications with variations on the ConcordanceSearcher, it has been
 * useful to factor out the getCharOffsetRequests.
 * <p>
 * This class should be used for functionality that is generally useful for
 * concordance searching.
 */
public class ConcordanceSearcherUtil {


  /**
   * Simple utility method to build a TokenCharOffsetRequests object
   * from a list of desired tokenOffsets, the number of tokensBefore
   * and the number of tokensAfter.
   *
   * @param tokenOffsets the tokenOffsets that are desired
   * @param tokensBefore the number of tokens before a desired tokenOffset
   * @param tokensAfter  the number of tokens after a desired tokenOffset
   * @param requests     an empty requests to be filled in
   */
  public static void getCharOffsetRequests(
      List<OffsetAttribute> tokenOffsets,
      int tokensBefore, int tokensAfter,
      TokenCharOffsetRequests requests) {

    for (OffsetAttribute tokenOffset : tokenOffsets) {
      int start = tokenOffset.startOffset() - tokensBefore;
      start = (start < 0) ? 0 : start;
      int end = tokenOffset.endOffset() + tokensAfter + 1;
      for (int i = start; i < end; i++) {
        requests.add(i);
      }
    }
  }

}
