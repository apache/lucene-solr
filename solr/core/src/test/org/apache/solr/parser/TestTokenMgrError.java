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
package org.apache.solr.parser;

import org.junit.Test;
import static org.junit.Assert.assertTrue;

/**
 * 
 * {@link TokenMgrError}
 */
public class TestTokenMgrError {

  /**
   * 
   * {@link TokenMgrError#addEscapes(String)}
   */
  @Test
  public void testAddEscapes() {
    final String stringWithSingleUnescapedCharacter = "\" \t \b \n \r \f \' \\";
    final String stringWithDoubleUnescapedCharacter = "\\\" \\t \\b \\n \\r \\f \\' \\\\";
    assertTrue(stringWithDoubleUnescapedCharacter
        .equalsIgnoreCase(TokenMgrError.addEscapes(stringWithSingleUnescapedCharacter)));

    final String notEscapedCharacs = "This won t be modified";
    assertTrue(notEscapedCharacs.equalsIgnoreCase(TokenMgrError.addEscapes(notEscapedCharacs)));

    final String unicode = "® § × ū è é";
    final String unicodeTransformedExpected = "\\u00ae \\u00a7 \\u00d7 \\u016b \\u00e8 \\u00e9";
    assertTrue(unicodeTransformedExpected.equalsIgnoreCase(TokenMgrError.addEscapes(unicode)));
  }

  @Test
  public void testLexicalError() {
    assertTrue("Lexical error at line 0, column 0.  Encountered: <EOF> after : \"Error After\""
        .equalsIgnoreCase(TokenMgrError.LexicalError(true, 0, 0, 0, "Error After", 'c')));
  }
}
