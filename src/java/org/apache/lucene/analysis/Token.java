package org.apache.lucene.analysis;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** A Token is an occurence of a term from the text of a field.  It consists of
  a term's text, the start and end offset of the term in the text of the field,
  and a type string.

  The start and end offsets permit applications to re-associate a token with
  its source text, e.g., to display highlighted query terms in a document
  browser, or to show matching text fragments in a KWIC (KeyWord In Context)
  display, etc.

  The type is an interned string, assigned by a lexical analyzer
  (a.k.a. tokenizer), naming the lexical or syntactic class that the token
  belongs to.  For example an end of sentence marker token might be implemented
  with type "eos".  The default token type is "word".  */

public class Token implements Cloneable {
  String termText;				  // the text of the term
  int startOffset;				  // start in source text
  int endOffset;				  // end in source text
  String type = "word";				  // lexical type

  private int positionIncrement = 1;

  /** Constructs a Token with the given term text, and start & end offsets.
      The type defaults to "word." */
  public Token(String text, int start, int end) {
    termText = text;
    startOffset = start;
    endOffset = end;
  }

  /** Constructs a Token with the given text, start and end offsets, & type. */
  public Token(String text, int start, int end, String typ) {
    termText = text;
    startOffset = start;
    endOffset = end;
    type = typ;
  }

  /** Set the position increment.  This determines the position of this token
   * relative to the previous Token in a {@link TokenStream}, used in phrase
   * searching.
   *
   * <p>The default value is one.
   *
   * <p>Some common uses for this are:<ul>
   *
   * <li>Set it to zero to put multiple terms in the same position.  This is
   * useful if, e.g., a word has multiple stems.  Searches for phrases
   * including either stem will match.  In this case, all but the first stem's
   * increment should be set to zero: the increment of the first instance
   * should be one.  Repeating a token with an increment of zero can also be
   * used to boost the scores of matches on that token.
   *
   * <li>Set it to values greater than one to inhibit exact phrase matches.
   * If, for example, one does not want phrases to match across removed stop
   * words, then one could build a stop word filter that removes stop words and
   * also sets the increment to the number of stop words removed before each
   * non-stop word.  Then exact phrase queries will only match when the terms
   * occur with no intervening stop words.
   *
   * </ul>
   * @see org.apache.lucene.index.TermPositions
   */
  public void setPositionIncrement(int positionIncrement) {
    if (positionIncrement < 0)
      throw new IllegalArgumentException
        ("Increment must be zero or greater: " + positionIncrement);
    this.positionIncrement = positionIncrement;
  }

  /** Returns the position increment of this Token.
   * @see #setPositionIncrement
   */
  public int getPositionIncrement() { return positionIncrement; }

  /** Sets the Token's term text. */
  public void setTermText(String text) {
    termText = text;
  }

  /** Returns the Token's term text. */
  public final String termText() { return termText; }

  /** Returns this Token's starting offset, the position of the first character
    corresponding to this token in the source text.

    Note that the difference between endOffset() and startOffset() may not be
    equal to termText.length(), as the term text may have been altered by a
    stemmer or some other filter. */
  public final int startOffset() { return startOffset; }

  /** Returns this Token's ending offset, one greater than the position of the
    last character corresponding to this token in the source text. */
  public final int endOffset() { return endOffset; }

  /** Returns this Token's lexical type.  Defaults to "word". */
  public final String type() { return type; }

  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("(" + termText + "," + startOffset + "," + endOffset);
    if (!type.equals("word"))
      sb.append(",type="+type);
    if (positionIncrement != 1)
      sb.append(",posIncr="+positionIncrement);
    sb.append(")");
    return sb.toString();
  }

  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e); // shouldn't happen since we implement Cloneable
    }
  }
}
