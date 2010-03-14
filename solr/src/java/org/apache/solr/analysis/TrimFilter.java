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

package org.apache.solr.analysis;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

import java.io.IOException;

/**
 * Trims leading and trailing whitespace from Tokens in the stream.
 *
 * @version $Id:$
 */
public final class TrimFilter extends TokenFilter {

  final boolean updateOffsets;
  private final TermAttribute termAtt;
  private final OffsetAttribute offsetAtt;


  public TrimFilter(TokenStream in, boolean updateOffsets) {
    super(in);
    this.updateOffsets = updateOffsets;

    this.termAtt = (TermAttribute) addAttribute(TermAttribute.class);
    this.offsetAtt = (OffsetAttribute) addAttribute(OffsetAttribute.class);
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (!input.incrementToken()) return false;

    char[] termBuffer = termAtt.termBuffer();
    int len = termAtt.termLength();
    //TODO: Is this the right behavior or should we return false?  Currently, "  ", returns true, so I think this should
    //also return true
    if (len == 0){
      return true;
    }
    int start = 0;
    int end = 0;
    int endOff = 0;

    // eat the first characters
    //QUESTION: Should we use Character.isWhitespace() instead?
    for (start = 0; start < len && termBuffer[start] <= ' '; start++) {
    }
    // eat the end characters
    for (end = len; end >= start && termBuffer[end - 1] <= ' '; end--) {
      endOff++;
    }
    if (start > 0 || end < len) {
      if (start < end) {
        termAtt.setTermBuffer(termBuffer, start, (end - start));
      } else {
        termAtt.setTermLength(0);
      }
      if (updateOffsets) {
        int newStart = offsetAtt.startOffset()+start;
        int newEnd = offsetAtt.endOffset() - (start<end ? endOff:0);
        offsetAtt.setOffset(newStart, newEnd);
      }
    }

    return true;
  }
}
