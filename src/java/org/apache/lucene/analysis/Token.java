package org.apache.lucene.analysis;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import org.apache.lucene.index.TermPositions;

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

public final class Token {
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
   * @see TermPositions
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

}
