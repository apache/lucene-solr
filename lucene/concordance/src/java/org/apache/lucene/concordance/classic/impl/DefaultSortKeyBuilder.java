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

package org.apache.lucene.concordance.classic.impl;

import java.util.Map;

import org.apache.lucene.concordance.charoffsets.RandomAccessCharOffsetContainer;
import org.apache.lucene.concordance.classic.ConcordanceSortKey;
import org.apache.lucene.concordance.classic.ConcordanceSortOrder;
import org.apache.lucene.concordance.classic.SortKeyBuilder;

/**
 * Builds basic sort key for the values available in ConcordanceSortOrder
 */
public class DefaultSortKeyBuilder implements SortKeyBuilder {

  private final static String SPACE = " ";
  private final static String EMPTY_STRING = "";
  //what filler to use when a "term" comes back as null from the
  //TokenCharOffsetResults
  private static String NULL_FILLER = "";
  private final ConcordanceSortOrder sortOrder;

  /**
   * Calls {@link #DefaultSortKeyBuilder(ConcordanceSortOrder)}
   * with value of: ConcordanceSortOrder.PRE
   */
  public DefaultSortKeyBuilder() {
    this.sortOrder = ConcordanceSortOrder.PRE;
  }

  /**
   * @param sortOrder sort order to use
   */
  public DefaultSortKeyBuilder(ConcordanceSortOrder sortOrder) {
    this.sortOrder = sortOrder;
  }

  @Override
  public ConcordanceSortKey buildKey(String docKey,
                                     int startTargetTokenOffset,
                                     int endTargetTokenOffset,
                                     RandomAccessCharOffsetContainer charOffsets,
                                     int tokensBefore, int tokensAfter,
                                     Map<String, String> metadata) {

    if (sortOrder == ConcordanceSortOrder.NONE) {
      return new ConcordanceSortKey(EMPTY_STRING);
    }

    if (sortOrder == ConcordanceSortOrder.DOC) {
      int targCharStart = charOffsets.getCharacterOffsetStart(startTargetTokenOffset);
      return new DocumentOrderSortKey(docKey, targCharStart);
    }

    StringBuilder sb = new StringBuilder();
    //order is important for appending to sb, target must come before pre/post
    if (sortOrder == ConcordanceSortOrder.TARGET_POST
        || sortOrder == ConcordanceSortOrder.TARGET_PRE) {

      for (int i = startTargetTokenOffset; i <= endTargetTokenOffset; i++) {
        String tmp = charOffsets.getTerm(i);
        if (tmp != null && tmp.length() > 0) {
          sb.append(tmp).append(SPACE);
        } else {
          sb.append(NULL_FILLER);
        }
      }
    }
    if (sortOrder == ConcordanceSortOrder.PRE
        || sortOrder == ConcordanceSortOrder.TARGET_PRE) {
      int tmpStart = startTargetTokenOffset - 1;
      int tmpEnd = Math.max(0, startTargetTokenOffset - tokensBefore);
      if (tmpStart < 0) {
        sb.append(SPACE);
      }

      for (int i = tmpStart; i >= tmpEnd; i--) {
        String tmp = charOffsets.getTerm(i);
        if (tmp != null && tmp.length() > 0) {
          sb.append(tmp).append(SPACE);
        } else {
          sb.append(NULL_FILLER);
        }
      }

    } else if (sortOrder == ConcordanceSortOrder.POST
        || sortOrder == ConcordanceSortOrder.TARGET_POST) {

      int tmpStart = endTargetTokenOffset + 1;
      int tmpEnd = Math.min(charOffsets.getLast(), endTargetTokenOffset + tokensAfter);

      if (tmpStart > charOffsets.getLast()) {
        sb.append(SPACE);
      }
      for (int i = tmpStart; i <= tmpEnd; i++) {
        String tmp = charOffsets.getTerm(i);
        if (tmp != null && tmp.length() > 0) {
          sb.append(tmp).append(SPACE);
        } else {
          sb.append(NULL_FILLER);
        }
      }
    }
    return new ConcordanceSortKey(sb.toString().trim());
  }

  @Override
  public boolean requiresAnalysisOfPre() {
    if (sortOrder == ConcordanceSortOrder.PRE
        || sortOrder == ConcordanceSortOrder.TARGET_PRE) {
      return true;
    }
    return false;
  }

  @Override
  public boolean requiresAnalysisOfPost() {
    if (sortOrder == ConcordanceSortOrder.POST
        || sortOrder == ConcordanceSortOrder.TARGET_POST) {
      return true;
    }
    return false;
  }

  @Override
  public boolean requiresAnalysisOfTarget() {
    if (sortOrder == ConcordanceSortOrder.TARGET_PRE
        || sortOrder == ConcordanceSortOrder.TARGET_POST) {
      return true;
    }
    return false;
  }

}
