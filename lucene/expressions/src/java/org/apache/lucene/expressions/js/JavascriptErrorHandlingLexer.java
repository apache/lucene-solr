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
package org.apache.lucene.expressions.js;


import java.text.ParseException;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.LexerNoViableAltException;
import org.antlr.v4.runtime.misc.Interval;

/**
 * Overrides the ANTLR 4 generated JavascriptLexer to allow for proper error handling
 */
class JavascriptErrorHandlingLexer extends JavascriptLexer {
  /**
   * Constructor for JavascriptErrorHandlingLexer
   * @param charStream the stream for the source text
   */
  public JavascriptErrorHandlingLexer(CharStream charStream) {
    super(charStream);
  }

  /**
   * Ensures the ANTLR lexer will throw an exception after the first error
   * @param lnvae the lexer exception
   */
  @Override
  public void recover(LexerNoViableAltException lnvae) {
    CharStream charStream = lnvae.getInputStream();
    int startIndex = lnvae.getStartIndex();
    String text = charStream.getText(Interval.of(startIndex, charStream.index()));

    ParseException parseException = new ParseException("unexpected character '" + getErrorDisplay(text) + "'" +
        " on line (" + _tokenStartLine + ") position (" + _tokenStartCharPositionInLine + ")", _tokenStartCharIndex);
    parseException.initCause(lnvae);
    throw new RuntimeException(parseException);
  }
}
