package org.apache.lucene.index;

import java.io.Serializable;

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
 * The TermVectorOffsetInfo class holds information pertaining to a Term in a {@link org.apache.lucene.index.TermPositionVector}'s
 * offset information.  This offset information is the character offset as set during the Analysis phase (and thus may not be the actual offset in the
 * original content).
 */
public class TermVectorOffsetInfo implements Serializable {
  /**
   * Convenience declaration when creating a {@link org.apache.lucene.index.TermPositionVector} that stores only position information.
   */
  public transient static final TermVectorOffsetInfo[] EMPTY_OFFSET_INFO = new TermVectorOffsetInfo[0];
  private int startOffset;
  private int endOffset;

  public TermVectorOffsetInfo() {
  }

  public TermVectorOffsetInfo(int startOffset, int endOffset) {
    this.endOffset = endOffset;
    this.startOffset = startOffset;
  }

  /**
   * The accessor for the ending offset for the term
   * @return The offset
   */
  public int getEndOffset() {
    return endOffset;
  }

  public void setEndOffset(int endOffset) {
    this.endOffset = endOffset;
  }

  /**
   * The accessor for the starting offset of the term.
   *
   * @return The offset
   */
  public int getStartOffset() {
    return startOffset;
  }

  public void setStartOffset(int startOffset) {
    this.startOffset = startOffset;
  }

  /**
   * Two TermVectorOffsetInfos are equals if both the start and end offsets are the same
   * @param o The comparison Object
   * @return true if both {@link #getStartOffset()} and {@link #getEndOffset()} are the same for both objects.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TermVectorOffsetInfo)) return false;

    final TermVectorOffsetInfo termVectorOffsetInfo = (TermVectorOffsetInfo) o;

    if (endOffset != termVectorOffsetInfo.endOffset) return false;
    if (startOffset != termVectorOffsetInfo.startOffset) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result;
    result = startOffset;
    result = 29 * result + endOffset;
    return result;
  }
}
