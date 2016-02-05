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
 * Lets all tokens pass through until it sees one with a start offset &lt;= a
 * configured limit, which won't pass and ends the stream.  This can be useful to
 * limit highlighting, for example.
 * <p>
 * By default, this filter ignores any tokens in the wrapped {@code TokenStream}
 * once the limit has been exceeded, which can result in {@code reset()} being
 * called prior to {@code incrementToken()} returning {@code false}.  For most
 * {@code TokenStream} implementations this should be acceptable, and faster
 * then consuming the full stream. If you are wrapping a {@code TokenStream}
 * which requires that the full stream of tokens be exhausted in order to
 * function properly, use the
 * {@link #LimitTokenOffsetFilter(TokenStream, int, boolean)} option.
 */
public final class LimitTokenOffsetFilter extends TokenFilter {

  private final OffsetAttribute offsetAttrib = addAttribute(OffsetAttribute.class);
  private int maxStartOffset;
  private final boolean consumeAllTokens;

  // some day we may limit by end offset too but no need right now

  /**
   * Lets all tokens pass through until it sees one with a start offset &lt;= {@code maxStartOffset}
   * which won't pass and ends the stream. It won't consume any tokens afterwards.
   *
   * @param maxStartOffset the maximum start offset allowed
   */
  public LimitTokenOffsetFilter(TokenStream input, int maxStartOffset) {
    this(input, maxStartOffset, false);
  }

  public LimitTokenOffsetFilter(TokenStream input, int maxStartOffset, boolean consumeAllTokens) {
    super(input);
    if (maxStartOffset < 0) {
      throw new IllegalArgumentException("maxStartOffset must be >= zero");
    }
    this.maxStartOffset = maxStartOffset;
    this.consumeAllTokens = consumeAllTokens;
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (!input.incrementToken()) {
      return false;
    }
    if (offsetAttrib.startOffset() <= maxStartOffset) {
      return true;
    }
    if (consumeAllTokens) {
      while (input.incrementToken()) {
        // no-op
      }
    }
    return false;
  }
}
