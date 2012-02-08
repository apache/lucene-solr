package org.apache.lucene.analysis.tokenattributes;

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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.AttributeImpl;

/**
 *This attribute can be used to mark a token as a keyword. Keyword aware
 * {@link TokenStream}s can decide to modify a token based on the return value
 * of {@link #isKeyword()} if the token is modified. Stemming filters for
 * instance can use this attribute to conditionally skip a term if
 * {@link #isKeyword()} returns <code>true</code>.
 */
public final class KeywordAttributeImpl extends AttributeImpl implements
    KeywordAttribute {
  private boolean keyword;

  @Override
  public void clear() {
    keyword = false;
  }

  @Override
  public void copyTo(AttributeImpl target) {
    KeywordAttribute attr = (KeywordAttribute) target;
    attr.setKeyword(keyword);
  }

  @Override
  public int hashCode() {
    return keyword ? 31 : 37;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (getClass() != obj.getClass())
      return false;
    final KeywordAttributeImpl other = (KeywordAttributeImpl) obj;
    return keyword == other.keyword;
  }

  /**
   * Returns <code>true</code> iff the current token is a keyword, otherwise
   * <code>false</code>/
   * 
   * @return <code>true</code> iff the current token is a keyword, otherwise
   *         <code>false</code>/
   */
  public boolean isKeyword() {
    return keyword;
  }

  /**
   * Marks the current token as keyword iff set to <code>true</code>.
   * 
   * @param isKeyword
   *          <code>true</code> iff the current token is a keyword, otherwise
   *          <code>false</code>.
   */
  public void setKeyword(boolean isKeyword) {
    keyword = isKeyword;
  }

}
