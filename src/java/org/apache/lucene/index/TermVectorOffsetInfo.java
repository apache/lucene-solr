package org.apache.lucene.index;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class TermVectorOffsetInfo {
    public static final TermVectorOffsetInfo [] EMPTY_OFFSET_INFO = new TermVectorOffsetInfo[0];
    private int startOffset;
    private int endOffset;

  public TermVectorOffsetInfo() {
  }

  public TermVectorOffsetInfo(int startOffset, int endOffset) {
    this.endOffset = endOffset;
    this.startOffset = startOffset;
  }

  public int getEndOffset() {
    return endOffset;
  }

  public void setEndOffset(int endOffset) {
    this.endOffset = endOffset;
  }

  public int getStartOffset() {
    return startOffset;
  }

  public void setStartOffset(int startOffset) {
    this.startOffset = startOffset;
  }

  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TermVectorOffsetInfo)) return false;

    final TermVectorOffsetInfo termVectorOffsetInfo = (TermVectorOffsetInfo) o;

    if (endOffset != termVectorOffsetInfo.endOffset) return false;
    if (startOffset != termVectorOffsetInfo.startOffset) return false;

    return true;
  }

  public int hashCode() {
    int result;
    result = startOffset;
    result = 29 * result + endOffset;
    return result;
  }
}
