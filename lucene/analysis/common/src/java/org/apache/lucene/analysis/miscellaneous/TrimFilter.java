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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.Version;

import java.io.IOException;

/**
 * Trims leading and trailing whitespace from Tokens in the stream.
 * <p>As of Lucene 4.4, this filter does not support updateOffsets=true anymore
 * as it can lead to broken token streams.
 */
public final class TrimFilter extends TokenFilter {

  final boolean updateOffsets;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  /**
   * Create a new {@link TrimFilter}.
   * @param version       the Lucene match version
   * @param in            the stream to consume
   * @param updateOffsets whether to update offsets
   * @deprecated Offset updates are not supported anymore as of Lucene 4.4.
   */
  @Deprecated
  public TrimFilter(Version version, TokenStream in, boolean updateOffsets) {
    super(in);
    if (updateOffsets && version.onOrAfter(Version.LUCENE_4_4)) {
      throw new IllegalArgumentException("updateOffsets=true is not supported anymore as of Lucene 4.4");
    }
    this.updateOffsets = updateOffsets;
  }

  /** Create a new {@link TrimFilter} on top of <code>in</code>. */
  public TrimFilter(TokenStream in) {
    super(in);
    this.updateOffsets = false;
  }

  /**
   * @deprecated Use {@link #TrimFilter(TokenStream)}
   */
  @Deprecated
  public TrimFilter(Version version, TokenStream in) {
    this(version, in, false);
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (!input.incrementToken()) return false;

    char[] termBuffer = termAtt.buffer();
    int len = termAtt.length();
    //TODO: Is this the right behavior or should we return false?  Currently, "  ", returns true, so I think this should
    //also return true
    if (len == 0){
      return true;
    }
    int start = 0;
    int end = 0;
    int endOff = 0;

    // eat the first characters
    for (start = 0; start < len && Character.isWhitespace(termBuffer[start]); start++) {
    }
    // eat the end characters
    for (end = len; end >= start && Character.isWhitespace(termBuffer[end - 1]); end--) {
      endOff++;
    }
    if (start > 0 || end < len) {
      if (start < end) {
        termAtt.copyBuffer(termBuffer, start, (end - start));
      } else {
        termAtt.setEmpty();
      }
      if (updateOffsets && len == offsetAtt.endOffset() - offsetAtt.startOffset()) {
        int newStart = offsetAtt.startOffset()+start;
        int newEnd = offsetAtt.endOffset() - (start<end ? endOff:0);
        offsetAtt.setOffset(newStart, newEnd);
      }
    }

    return true;
  }
}
