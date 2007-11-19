package org.apache.lucene.analysis;

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

import org.apache.lucene.index.Payload;
import org.apache.lucene.index.TermPositions;

/** A Token is an occurence of a term from the text of a field.  It consists of
  a term's text, the start and end offset of the term in the text of the field,
  and a type string.
  <p>
  The start and end offsets permit applications to re-associate a token with
  its source text, e.g., to display highlighted query terms in a document
  browser, or to show matching text fragments in a KWIC (KeyWord In Context)
  display, etc.
  <p>
  The type is an interned string, assigned by a lexical analyzer
  (a.k.a. tokenizer), naming the lexical or syntactic class that the token
  belongs to.  For example an end of sentence marker token might be implemented
  with type "eos".  The default token type is "word".  
  <p>
  A Token can optionally have metadata (a.k.a. Payload) in the form of a variable
  length byte array. Use {@link TermPositions#getPayloadLength()} and 
  {@link TermPositions#getPayload(byte[], int)} to retrieve the payloads from the index.
  
  <br><br>
  <p><font color="#FF0000">
  WARNING: The status of the <b>Payloads</b> feature is experimental. 
  The APIs introduced here might change in the future and will not be 
  supported anymore in such a case.</font>

  <br><br>

  <p><b>NOTE:</b> As of 2.3, Token stores the term text
  internally as a malleable char[] termBuffer instead of
  String termText.  The indexing code and core tokenizers
  have been changed re-use a single Token instance, changing
  its buffer and other fields in-place as the Token is
  processed.  This provides substantially better indexing
  performance as it saves the GC cost of new'ing a Token and
  String for every term.  The APIs that accept String
  termText are still available but a warning about the
  associated performance cost has been added (below).  The
  {@link #termText()} method has been deprecated.</p>
  
  <p>Tokenizers and filters should try to re-use a Token
  instance when possible for best performance, by
  implementing the {@link TokenStream#next(Token)} API.
  Failing that, to create a new Token you should first use
  one of the constructors that starts with null text.  Then
  you should call either {@link #termBuffer()} or {@link
  #resizeTermBuffer(int)} to retrieve the Token's
  termBuffer.  Fill in the characters of your term into this
  buffer, and finally call {@link #setTermLength(int)} to
  set the length of the term text.  See <a target="_top"
  href="https://issues.apache.org/jira/browse/LUCENE-969">LUCENE-969</a>
  for details.</p>

  @see org.apache.lucene.index.Payload
*/

// TODO: Remove warning after API has been finalized

public class Token implements Cloneable {

  private static final String DEFAULT_TYPE = "word";
  private static int MIN_BUFFER_SIZE = 10;

  /** @deprecated: we will remove this when we remove the
   * deprecated APIs */
  private String termText;

  char[] termBuffer;                              // characters for the term text
  int termLength;                                 // length of term text in buffer

  int startOffset;				  // start in source text
  int endOffset;				  // end in source text
  String type = DEFAULT_TYPE;                     // lexical type
  
  Payload payload;
  
  int positionIncrement = 1;

  /** Constructs a Token will null text. */
  public Token() {
  }

  /** Constructs a Token with null text and start & end
   *  offsets.
   *  @param start start offset
   *  @param end end offset */
  public Token(int start, int end) {
    startOffset = start;
    endOffset = end;
  }

  /** Constructs a Token with null text and start & end
   *  offsets plus the Token type.
   *  @param start start offset
   *  @param end end offset */
  public Token(int start, int end, String typ) {
    startOffset = start;
    endOffset = end;
    type = typ;
  }

  /** Constructs a Token with the given term text, and start
   *  & end offsets.  The type defaults to "word."
   *  <b>NOTE:</b> for better indexing speed you should
   *  instead use the char[] termBuffer methods to set the
   *  term text.
   *  @param text term text
   *  @param start start offset
   *  @param end end offset */
  public Token(String text, int start, int end) {
    termText = text;
    startOffset = start;
    endOffset = end;
  }

  /** Constructs a Token with the given text, start and end
   *  offsets, & type.  <b>NOTE:</b> for better indexing
   *  speed you should instead use the char[] termBuffer
   *  methods to set the term text.
   *  @param text term text
   *  @param start start offset
   *  @param end end offset
   *  @param typ token type */
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
  public int getPositionIncrement() {
    return positionIncrement;
  }

  /** Sets the Token's term text.  <b>NOTE:</b> for better
   *  indexing speed you should instead use the char[]
   *  termBuffer methods to set the term text. */
  public void setTermText(String text) {
    termText = text;
    termBuffer = null;
  }

  /** Returns the Token's term text.
   * 
   * @deprecated Use {@link #termBuffer()} and {@link
   * #termLength()} instead. */
  public final String termText() {
    if (termText == null && termBuffer != null)
      termText = new String(termBuffer, 0, termLength);
    return termText;
  }

  /** Copies the contents of buffer, starting at offset for
   *  length characters, into the termBuffer
   *  array. <b>NOTE:</b> for better indexing speed you
   *  should instead retrieve the termBuffer, using {@link
   *  #termBuffer()} or {@link #resizeTermBuffer(int)}, and
   *  fill it in directly to set the term text.  This saves
   *  an extra copy. */
  public final void setTermBuffer(char[] buffer, int offset, int length) {
    resizeTermBuffer(length);
    System.arraycopy(buffer, offset, termBuffer, 0, length);
    termLength = length;
  }

  /** Returns the internal termBuffer character array which
   *  you can then directly alter.  If the array is too
   *  small for your token, use {@link
   *  #resizeTermBuffer(int)} to increase it.  After
   *  altering the buffer be sure to call {@link
   *  #setTermLength} to record the number of valid
   *  characters that were placed into the termBuffer. */
  public final char[] termBuffer() {
    initTermBuffer();
    return termBuffer;
  }

  /** Grows the termBuffer to at least size newSize.
   *  @param newSize minimum size of the new termBuffer
   *  @return newly created termBuffer with length >= newSize
   */
  public char[] resizeTermBuffer(int newSize) {
    initTermBuffer();
    if (newSize > termBuffer.length) {
      int size = termBuffer.length;
      while(size < newSize)
        size *= 2;
      char[] newBuffer = new char[size];
      System.arraycopy(termBuffer, 0, newBuffer, 0, termBuffer.length);
      termBuffer = newBuffer;
    }
    return termBuffer;
  }

  // TODO: once we remove the deprecated termText() method
  // and switch entirely to char[] termBuffer we don't need
  // to use this method anymore
  private void initTermBuffer() {
    if (termBuffer == null) {
      if (termText == null) {
        termBuffer = new char[MIN_BUFFER_SIZE];
        termLength = 0;
      } else {
        int length = termText.length();
        if (length < MIN_BUFFER_SIZE) length = MIN_BUFFER_SIZE;
        termBuffer = new char[length];
        termLength = termText.length();
        termText.getChars(0, termText.length(), termBuffer, 0);
        termText = null;
      }
    } else if (termText != null)
      termText = null;
  }

  /** Return number of valid characters (length of the term)
   *  in the termBuffer array. */
  public final int termLength() {
    initTermBuffer();
    return termLength;
  }

  /** Set number of valid characters (length of the term) in
   *  the termBuffer array. */
  public final void setTermLength(int length) {
    initTermBuffer();
    termLength = length;
  }

  /** Returns this Token's starting offset, the position of the first character
    corresponding to this token in the source text.

    Note that the difference between endOffset() and startOffset() may not be
    equal to termText.length(), as the term text may have been altered by a
    stemmer or some other filter. */
  public final int startOffset() {
    return startOffset;
  }

  /** Set the starting offset.
      @see #startOffset() */
  public void setStartOffset(int offset) {
    this.startOffset = offset;
  }

  /** Returns this Token's ending offset, one greater than the position of the
    last character corresponding to this token in the source text. */
  public final int endOffset() {
    return endOffset;
  }

  /** Set the ending offset.
      @see #endOffset() */
  public void setEndOffset(int offset) {
    this.endOffset = offset;
  }

  /** Returns this Token's lexical type.  Defaults to "word". */
  public final String type() {
    return type;
  }

  /** Set the lexical type.
      @see #type() */
  public final void setType(String type) {
    this.type = type;
  }

  /** 
   * Returns this Token's payload. 
   * <p><font color="#FF0000">
   * WARNING: The status of the <b>Payloads</b> feature is experimental. 
   * The APIs introduced here might change in the future and will not be 
   * supported anymore in such a case.</font>
   */
  // TODO: Remove warning after API has been finalized
  public Payload getPayload() {
    return this.payload;
  }

  /** 
   * Sets this Token's payload.
   * <p><font color="#FF0000">
   * WARNING: The status of the <b>Payloads</b> feature is experimental. 
   * The APIs introduced here might change in the future and will not be 
   * supported anymore in such a case.</font>
   */
  // TODO: Remove warning after API has been finalized
  public void setPayload(Payload payload) {
    this.payload = payload;
  }
  
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("(");
    initTermBuffer();
    if (termBuffer == null)
      sb.append("null");
    else
      sb.append(termBuffer, 0, termLength);
    sb.append("," + startOffset + "," + endOffset);
    if (!type.equals("word"))
      sb.append(",type="+type);
    if (positionIncrement != 1)
      sb.append(",posIncr="+positionIncrement);
    sb.append(")");
    return sb.toString();
  }

  /** Resets the term text, payload, and positionIncrement to default.
   * Other fields such as startOffset, endOffset and the token type are
   * not reset since they are normally overwritten by the tokenizer. */
  public void clear() {
    payload = null;
    // Leave termBuffer to allow re-use
    termLength = 0;
    termText = null;
    positionIncrement = 1;
    // startOffset = endOffset = 0;
    // type = DEFAULT_TYPE;
  }

  public Object clone() {
    try {
      Token t = (Token)super.clone();
      if (termBuffer != null) {
        t.termBuffer = null;
        t.setTermBuffer(termBuffer, 0, termLength);
      }
      return t;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);  // shouldn't happen
    }
  }
}
