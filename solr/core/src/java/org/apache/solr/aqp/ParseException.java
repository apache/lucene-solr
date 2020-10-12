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
 * This exception is thrown when parse errors are encountered.
 * You can explicitly create objects of this exception type by
 * calling the method generateParseException in the generated
 * parser.
 *
 * You can modify this class to customize your error reporting
 * mechanisms so long as you retain the public fields.
 */
@SuppressWarnings({"FieldCanBeLocal", "unused"}) // unnecessary fields used for doc, useful in debugger
public class ParseException extends Exception {

  /**
   * The version identifier for this Serializable class.
   * Increment only if the <i>serialized</i> form of the
   * class changes.
   */
  private static final long serialVersionUID = 1L;

  /**
   * This constructor is used by the method "generateParseException"
   * in the generated parser.  Calling this constructor generates
   * a new object of this type with the fields "currentToken",
   * "expectedTokenSequences", and "tokenImage" set.
   */
  public ParseException(Token currentTokenVal,
                        int[][] expectedTokenSequencesVal,
                        String[] tokenImageVal
                       )
  {
    super(initialise(currentTokenVal, expectedTokenSequencesVal, tokenImageVal));
    currentToken = currentTokenVal;
    expectedTokenSequences = expectedTokenSequencesVal;
    tokenImage = tokenImageVal;
  }

  /**
   * The following constructors are for use by you for whatever
   * purpose you can think of.  Constructing the exception in this
   * manner makes the exception behave in the normal way - i.e., as
   * documented in the class "Throwable".  The fields "errorToken",
   * "expectedTokenSequences", and "tokenImage" do not contain
   * relevant information.  The JavaCC generated code does not use
   * these constructors.
   */

  public ParseException() {
    super();
  }

  /** Constructor with message. */
  public ParseException(String message) {
    super(message);
  }


  /**
   * This is the last token that has been consumed successfully.  If
   * this object has been created due to a parse error, the token
   * followng this token will (therefore) be the first error token.
   */
  private Token currentToken;

  /**
   * Each entry in this array is an array of integers.  Each array
   * of integers represents a sequence of tokens (by their ordinal
   * values) that is expected at this point of the parse.
   */
  private int[][] expectedTokenSequences;

  /**
   * This is a reference to the "tokenImage" array of the generated
   * parser within which the parse error occurred.  This array is
   * defined in the generated ...Constants interface.
   */
  private String[] tokenImage;

  /**
   * It uses "currentToken" and "expectedTokenSequences" to generate a parse
   * error message and returns it.  If this object has been created
   * due to a parse error, and you do not catch it (it gets thrown
   * from the parser) the correct error message
   * gets displayed.
   */
  private static String initialise(Token currentToken,
                           int[][] expectedTokenSequences,
                           String[] tokenImage) {
    String eol = System.getProperty("line.separator", "\n");
    StringBuilder expected = new StringBuilder();
    int maxSize = 0;
    for (int[] expectedTokenSequence : expectedTokenSequences) {
      if (maxSize < expectedTokenSequence.length) {
        maxSize = expectedTokenSequence.length;
      }
      for (int i : expectedTokenSequence) {
        expected.append(tokenImage[i]).append(' ');
      }
      if (expectedTokenSequence[expectedTokenSequence.length - 1] != 0) {
        expected.append("...");
      }
      expected.append(eol).append("    ");
    }
    StringBuilder retval = new StringBuilder("Encountered \"");
    Token tok = currentToken.next;
    for (int i = 0; i < maxSize; i++) {
      if (i != 0) retval.append(" ");
      if (tok.kind == 0) {
        retval.append(tokenImage[0]);
        break;
      }
      retval.append(" ").append(tokenImage[tok.kind]);
      retval.append(" \"");
      retval.append(Token.add_escapes(tok.image));
      retval.append(" \"");
      tok = tok.next;
    }
    retval.append("\" at line ").append(currentToken.next.beginLine).append(", column ").append(currentToken.next.beginColumn);
    retval.append(".").append(eol);
    if (expectedTokenSequences.length == 1) {
      retval.append("Was expecting:").append(eol).append("    ");
    } else {
      retval.append("Was expecting one of:").append(eol).append("    ");
    }
    retval.append(expected.toString());
    return retval.toString();
  }

  /**
   * The end of line string for this machine.
   */
  protected String eol = System.getProperty("line.separator", "\n");

}
/* JavaCC - OriginalChecksum=b88f34f85c77ab76c54ac16da6d39d73 (do not edit this line) */
