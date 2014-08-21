package org.apache.lucene.analysis.core;

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
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharacterUtils;
import org.apache.lucene.util.Version;

/**
 * Normalizes token text to UPPER CASE.
 * <a name="version"/>
 * <p>You may specify the {@link Version}
 * compatibility when creating UpperCaseFilter
 *
 * <p><b>NOTE:</b> In Unicode, this transformation may lose information when the
 * upper case character represents more than one lower case character. Use this filter
 * when you require uppercase tokens.  Use the {@link LowerCaseFilter} for 
 * general search matching
 */
public final class UpperCaseFilter extends TokenFilter {
  private final CharacterUtils charUtils;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  
  /**
   * Create a new UpperCaseFilter, that normalizes token text to upper case.
   * 
   * @param in TokenStream to filter
   */
  public UpperCaseFilter(TokenStream in) {
    super(in);
    charUtils = CharacterUtils.getInstance();
  }

  /**
   * @deprecated Use {@link #UpperCaseFilter(TokenStream)}
   */
  @Deprecated
  public UpperCaseFilter(Version matchVersion, TokenStream in) {
    super(in);
    charUtils = CharacterUtils.getInstance(matchVersion);
  }
  
  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      charUtils.toUpperCase(termAtt.buffer(), 0, termAtt.length());
      return true;
    } else
      return false;
  }
}
