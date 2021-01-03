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
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;

/** Allows for proper error handling in the ANTLR 4 parser */
class JavascriptParserErrorStrategy extends DefaultErrorStrategy {
  /**
   * Ensures the ANTLR parser will throw an exception after the first error
   *
   * @param recognizer the parser being used
   * @param re the original exception from the parser
   */
  @Override
  public void recover(Parser recognizer, RecognitionException re) {
    Token token = re.getOffendingToken();
    String message;

    if (token == null) {
      message = "error " + getTokenErrorDisplay(token);
    } else if (re instanceof InputMismatchException) {
      message =
          "unexpected token "
              + getTokenErrorDisplay(token)
              + " on line ("
              + token.getLine()
              + ") position ("
              + token.getCharPositionInLine()
              + ")"
              + " was expecting one of "
              + re.getExpectedTokens().toString(recognizer.getVocabulary());
    } else if (re instanceof NoViableAltException) {
      if (token.getType() == JavascriptParser.EOF) {
        message = "unexpected end of expression";
      } else {
        message =
            "invalid sequence of tokens near "
                + getTokenErrorDisplay(token)
                + " on line ("
                + token.getLine()
                + ") position ("
                + token.getCharPositionInLine()
                + ")";
      }
    } else {
      message =
          " unexpected token near "
              + getTokenErrorDisplay(token)
              + " on line ("
              + token.getLine()
              + ") position ("
              + token.getCharPositionInLine()
              + ")";
    }

    ParseException parseException = new ParseException(message, token.getStartIndex());
    parseException.initCause(re);
    throw new RuntimeException(parseException);
  }

  /**
   * Ensures the ANTLR parser will throw an exception after the first error
   *
   * @param recognizer the parser being used
   * @return no actual return value
   * @throws RecognitionException not used as a ParseException wrapped in a RuntimeException is
   *     thrown instead
   */
  @Override
  public Token recoverInline(Parser recognizer) throws RecognitionException {
    Token token = recognizer.getCurrentToken();
    String message =
        "unexpected token "
            + getTokenErrorDisplay(token)
            + " on line ("
            + token.getLine()
            + ") position ("
            + token.getCharPositionInLine()
            + ")"
            + " was expecting one of "
            + recognizer.getExpectedTokens().toString(recognizer.getVocabulary());
    ParseException parseException = new ParseException(message, token.getStartIndex());
    throw new RuntimeException(parseException);
  }

  /**
   * Do not allow syncing after errors to ensure the ANTLR parser will throw an exception
   *
   * @param recognizer the parser being used
   */
  @Override
  public void sync(Parser recognizer) {}
}
