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
package org.apache.solr.aqp;

/**
 * Describes the input token stream.
 */

public class AdvToken extends Token {

  /**
   * The version identifier for this Serializable class.
   * Increment only if the <i>serialized</i> form of the
   * class changes.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Keep track of the type of span we're currently building.
   */
  public SpanContext value;

  /**
   * The number of unclosed left parens we've encountered before this token.
   * This is important for support of pattern'ed synonyms where the patterns
   * can include parenthesis (i.e. \d+\([a-z]\) to match 401(k) )
   * with out this and related code in the parser base class, the parenthesis
   * will be removed before the synonym filter can see them.
   */
  public int pDepth;

  /**
   * An optional attribute value of the Token.
   * Tokens which are not used as syntactic sugar will often contain
   * meaningful values that will be used later on by the compiler or
   * interpreter. This attribute value is often different from the image.
   * Any subclass of Token that actually wants to return a non-null value can
   * override this method as appropriate.
   */
  public SpanContext getValue() {
    return value;
  }

  /**
   * Constructs a new token for the specified Image and Kind.
   */
  AdvToken(int kind, String image)
  {
    super(kind,image);
  }

}
