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
import java.util.UUID;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;

/**
 * Allows Tokens with a given combination of flags to be dropped.
 *
 * @see DropIfFlaggedFilterFactory
 */
public class DropIfFlaggedFilter extends TokenFilter {

  private int dropFlags;

  private CharTermAttribute attribute = getAttribute(CharTermAttribute.class);
  private boolean firstToken = true;

  /**
   * Construct a token stream filtering the given input.
   *
   * @param input the source stream
   * @param dropFlags a combination of flags that indicates that the token should be dropped.
   */
  @SuppressWarnings("WeakerAccess")
  protected DropIfFlaggedFilter(TokenStream input, int dropFlags) {
    super(input);
    this.dropFlags = dropFlags;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    boolean result;
    boolean dropToken;
    do {
      result = input.incrementToken();
      dropToken = (getAttribute(FlagsAttribute.class).getFlags() & dropFlags) == dropFlags;
      if (firstToken && dropToken) {
        // we've attempted to drop the very first token. If the token had a synonym, this can cause:
        // java.lang.IllegalArgumentException: first position increment must be > 0 (got 0) for field '_text_'
        // Since there is no way to look ahead or behind in the stream, all we can do is convert to something
        // that will never match any other token. Thus we make it "un-findable" and therefore effectively
        // deleted from a user's perspective (ignoring an extremely low probability of duplication by chance)
        attribute.setEmpty();
        attribute.append(getRandomToken());
        dropToken = false;
      }
      firstToken = false;
    } while (result && dropToken);
    return result;
  }

  String getRandomToken() {
    // The following tactic is a hazard for threaded environments because of locking in SecureRandom, but
    // this case (where the very first token is dropped) should hopefully be an uncommon case for most users
    // and still result in low thread contention. An indexing or query situation with frequent drops of the first
    // token could suffer. Open to suggestions for a better method, possibly something involving ThreadLocalRandom
    // but some serious thought needs to go into how possible duplication is with such a change. Also of concern are
    // the cases where java uses a RNG that can run out of entropy and block... Erring on the side of correct
    // functionality over performance for now.
    return String.valueOf(UUID.randomUUID());
  }

  @Override
  public void reset() throws IOException {
    firstToken = true;
    super.reset();
  }
}
