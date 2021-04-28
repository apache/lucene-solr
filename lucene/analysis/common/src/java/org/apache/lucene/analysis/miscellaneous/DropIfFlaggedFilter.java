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

import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;

/**
 * Allows Tokens with a given combination of flags to be dropped. If all flags specified are present
 * the token is dropped, otherwise it is retained.
 *
 * @see DropIfFlaggedFilterFactory
 */
public class DropIfFlaggedFilter extends FilteringTokenFilter {
  private final FlagsAttribute flagsAtt = addAttribute(FlagsAttribute.class);
  private final int dropFlags;

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
  protected boolean accept() {
    return (flagsAtt.getFlags() & dropFlags) != dropFlags;
  }

}
