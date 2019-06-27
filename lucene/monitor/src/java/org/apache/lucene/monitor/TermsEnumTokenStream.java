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

package org.apache.lucene.monitor;

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;

/**
 * A TokenStream created from a {@link org.apache.lucene.index.TermsEnum}
 */
class TermsEnumTokenStream extends TokenStream {

  private final BytesRefIterator termsEnum;
  private final CharTermAttribute charTerm = addAttribute(CharTermAttribute.class);

  /**
   * Create a new TermsEnumTokenStream using a TermsEnum
   *
   * @param termsEnum the TermsEnum to convert
   */
  public TermsEnumTokenStream(BytesRefIterator termsEnum) {
    this.termsEnum = termsEnum;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    clearAttributes();
    BytesRef bytes = termsEnum.next();
    if (bytes == null)
      return false;
    charTerm.setEmpty();
    charTerm.append(bytes.utf8ToString());
    return true;
  }
}
