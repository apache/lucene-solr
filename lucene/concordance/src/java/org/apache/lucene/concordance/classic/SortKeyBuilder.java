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

package org.apache.lucene.concordance.classic;

import java.util.Map;

import org.apache.lucene.concordance.charoffsets.RandomAccessCharOffsetContainer;

public interface SortKeyBuilder {

  /**
   * Builds a sort key from the classic TokenCharOffsetResults object
   *
   * @param docKey                 to be used if sorting by document key
   * @param startTargetTokenOffset start target token offest
   * @param endTargetTokenOffset end target token offset
   * @param charOffsets charoffsets
   * @param numTokensPre number of tokens before
   * @param numTokensPost number of tokens after
   * @param metadata metadata
   * @return ConcordanceSortKey
   */
  ConcordanceSortKey buildKey(String docKey,
                              int startTargetTokenOffset, int endTargetTokenOffset,
                              RandomAccessCharOffsetContainer charOffsets,
                              int numTokensPre, int numTokensPost, Map<String, String> metadata);

  public boolean requiresAnalysisOfPre();

  public boolean requiresAnalysisOfPost();

  public boolean requiresAnalysisOfTarget();
}
