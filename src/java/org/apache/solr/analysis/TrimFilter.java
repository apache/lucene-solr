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

import java.io.IOException;

/**
 * Trims leading and trailing whitespace from Tokens in the stream.
 *
 * @version $Id:$
 */
public final class TrimFilter extends TokenFilter {

  final boolean updateOffsets;

  public TrimFilter(TokenStream in, boolean updateOffsets) {
    super(in);
    this.updateOffsets = updateOffsets;
  }

  @Override
  public final Token next(Token in) throws IOException {
    Token t = input.next(in);
    if (null == t || null == t.termBuffer() || t.termLength() == 0){
      return t;
    }
    char[] termBuffer = t.termBuffer();
    int len = t.termLength();
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
        t.setTermBuffer(t.termBuffer(), start, (end - start));
      } else {
        t.setTermLength(0);
      }
      if (updateOffsets) {
        t.setStartOffset(t.startOffset() + start);
        if (start < end) {
          t.setEndOffset(t.endOffset() - endOff);
        } //else if end is less than, start, then the term length is 0, so, no need to bother w/ the end offset
      }
      /*t = new Token( t.termText().substring( start, end ),
     t.startOffset()+start,
     t.endOffset()-endOff,
     t.type() );*/


    }

    return t;
  }
}
