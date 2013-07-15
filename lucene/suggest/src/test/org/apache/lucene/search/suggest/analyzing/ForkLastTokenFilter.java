package org.apache.lucene.search.suggest.analyzing;

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

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

/** Repeats the last token, if the endOffset indicates that
 *  the token didn't have any characters after it (i.e. it
 *  is not "done").  This is useful in analyzing
 *  suggesters along with StopKeywordFilter: imagine the
 *  user has typed 'a', but your stop filter would normally
 *  remove that.  This token filter will repeat that last a
 *  token, setting {@link KeywordAttribute}, so that the
 *  {@link StopKeywordFilter} won't remove it, and then
 *  suggestions starting with a will be shown.  */

final class ForkLastTokenFilter extends TokenFilter {

  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final KeywordAttribute keywordAtt = addAttribute(KeywordAttribute.class);
  private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);

  State lastToken;
  int maxEndOffset;
  boolean stop = false;

  public ForkLastTokenFilter(TokenStream in) {
    super(in);
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (stop) {
      return false;
    } else if (input.incrementToken()) {
      lastToken = captureState();
      maxEndOffset = Math.max(maxEndOffset, offsetAtt.endOffset());
      return true;
    } else if (lastToken == null) {
      return false;
    } else {

      // TODO: this is iffy!!!  maybe somehow instead caller
      // could tell us endOffset up front?
      input.end();

      if (offsetAtt.endOffset() == maxEndOffset) {
        // Text did not see end of token char:
        restoreState(lastToken);
        keywordAtt.setKeyword(true);
        posIncAtt.setPositionIncrement(0);
        lastToken = null;
        stop = true;
        return true;
      } else {
        return false;
      }
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    lastToken = null;
    maxEndOffset = -1;
    stop = false;
  }
}
