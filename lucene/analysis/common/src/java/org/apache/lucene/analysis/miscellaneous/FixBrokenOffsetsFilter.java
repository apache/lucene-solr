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

package org.apache.lucene.analysis.miscellaneous;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

/** 
 * A filter to correct offsets that illegally go backwards.
 *
 * @deprecated Fix the token filters that create broken offsets in the first place.
 */
@Deprecated
public final class FixBrokenOffsetsFilter extends TokenFilter {

  private int lastStartOffset;
  private int lastEndOffset;

  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  public FixBrokenOffsetsFilter(TokenStream in) {
    super(in);
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken() == false) {
      return false;
    }
    fixOffsets();
    return true;
  }

  @Override
  public void end() throws IOException {
    super.end();
    fixOffsets();
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    lastStartOffset = 0;
    lastEndOffset = 0;
  }

  private void fixOffsets() {
    int startOffset = offsetAtt.startOffset();
    int endOffset = offsetAtt.endOffset();
    if (startOffset < lastStartOffset) {
      startOffset = lastStartOffset;
    }
    if (endOffset < startOffset) {
      endOffset = startOffset;
    }
    offsetAtt.setOffset(startOffset, endOffset);
    lastStartOffset = startOffset;
    lastEndOffset = endOffset;
  }
}
