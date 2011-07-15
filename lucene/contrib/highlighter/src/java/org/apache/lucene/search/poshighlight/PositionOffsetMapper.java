package org.apache.lucene.search.poshighlight;

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

import org.apache.lucene.index.TermVectorMapper;
import org.apache.lucene.index.TermVectorOffsetInfo;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Create a map of position->offsets using term vectors.  TODO: In highlighting, we don't really need the
 * entire map; make a sparse map including only required positions.
 * 
 * @lucene.experimental
 */

public class PositionOffsetMapper extends TermVectorMapper {
  private int maxPos = 0;
  private static final int BUF_SIZE = 128;
  int startOffset[] = new int[BUF_SIZE], endOffset[] = new int[BUF_SIZE];   

  public void setExpectations(String field, int numTerms,
      boolean storeOffsets, boolean storePositions) {
  }

  public void map(BytesRef term, int frequency,
      TermVectorOffsetInfo[] offsets, int[] positions) 
  {
    for (int i = 0; i < positions.length; i++) {
      int pos = positions[i];
      if (pos >= startOffset.length) {
        grow (pos + BUF_SIZE);
        maxPos = pos;
      } else if (pos > maxPos) {
        maxPos = pos;
      }
      startOffset[pos] = offsets[i].getStartOffset();
      endOffset[pos] = offsets[i].getEndOffset();
    }
  }
  
  private void grow (int size) {
    startOffset = ArrayUtil.grow (startOffset, size);
    endOffset = ArrayUtil.grow (endOffset, size);
  }
  
  public int getStartOffset(int pos) {
    return startOffset[pos];
  }
  
  public int getEndOffset(int pos) {
    return endOffset[pos];
  }
  
  public int getMaxPosition() {
    return maxPos;
  }
}