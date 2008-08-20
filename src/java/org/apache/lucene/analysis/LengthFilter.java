package org.apache.lucene.analysis;

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

import java.io.IOException;

/**
 * Removes words that are too long and too short from the stream.
 *
 *
 * @version $Id$
 */
public final class LengthFilter extends TokenFilter {

  final int min;
  final int max;

  /**
   * Build a filter that removes words that are too long or too
   * short from the text.
   */
  public LengthFilter(TokenStream in, int min, int max)
  {
    super(in);
    this.min = min;
    this.max = max;
  }

  /**
   * Returns the next input Token whose term() is the right len
   */
  public final Token next(final Token reusableToken) throws IOException
  {
    assert reusableToken != null;
    // return the first non-stop word found
    for (Token nextToken = input.next(reusableToken); nextToken != null; nextToken = input.next(reusableToken))
    {
      int len = nextToken.termLength();
      if (len >= min && len <= max) {
          return nextToken;
      }
      // note: else we ignore it but should we index each part of it?
    }
    // reached EOS -- return null
    return null;
  }
}
