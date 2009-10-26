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

import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.index.Payload;
import org.apache.lucene.index.TermPositions;     // for javadoc
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeImpl;

/** 
  A Token is an occurrence of a term from the text of a field.  It consists of
  a term's text, the start and end offset of the term in the text of the field,
  and a type string.
  <p>
  The start and end offsets permit applications to re-associate a token with
  its source text, e.g., to display highlighted query terms in a document
  browser, or to show matching text fragments in a <abbr title="KeyWord In Context">KWIC</abbr>
  display, etc.
  <p>
  The type is a string, assigned by a lexical analyzer
  (a.k.a. tokenizer), naming the lexical or syntactic class that the token
  belongs to.  For example an end of sentence marker token might be implemented
  with type "eos".  The default token type is "word".  
  <p>
  A Token can optionally have metadata (a.k.a. Payload) in the form of a variable
  length byte array. Use {@link TermPositions#getPayloadLength()} and 
  {@link TermPositions#getPayload(byte[], int)} to retrieve the payloads from the index.
  
  <br><br>
  
  <p><b>NOTE:</b> As of 2.9, Token implements all {@link Attribute} interfaces
  that are part of core Lucene and can be found in the {@code tokenattributes} subpackage.
  Even though it is not necessary to use Token anymore, with the new TokenStream API it can
  be used as convenience class that implements all {@link Attribute}s, which is especially useful
  to easily switch from the old to the new TokenStream API.
  
  <br><br>

  <p><b>NOTE:</b> As of 2.3, Token stores the term text
  internally as a malleable char[] termBuffer instead of
  String termText.  The indexing code and core tokenizers
  have been changed to re-use a single Token instance, changing
  its buffer and other fields in-place as the Token is
  processed.  This provides substantially better indexing
  performance as it saves the GC cost of new'ing a Token and
  String for every term.  The APIs that accept String
  termText are still available but a warning about the
  associated performance cost has been added (below).  The
  {@link #termText()} method has been deprecated.</p>
  
  <p>Tokenizers and TokenFilters should try to re-use a Token
  instance when possible for best performance, by
  implementing the {@link TokenStream#incrementToken()} API.
  Failing that, to create a new Token you should first use
  one of the constructors that starts with null text.  To load
  the token from a char[] use {@link #setTermBuffer(char[], int, int)}.
  To load from a String use {@link #setTermBuffer(String)} or {@link #setTermBuffer(String, int, int)}.
  Alternatively you can get the Token's termBuffer by calling either {@link #termBuffer()},
  if you know that your text is shorter than the capacity of the termBuffer
  or {@link #resizeTermBuffer(int)}, if there is any possibility
  that you may need to grow the buffer. Fill in the characters of your term into this
  buffer, with {@link String#getChars(int, int, char[], int)} if loading from a string,
  or with {@link System#arraycopy(Object, int, Object, int, int)}, and finally call {@link #setTermLength(int)} to
  set the length of the term text.  See <a target="_top"
  href="https://issues.apache.org/jira/browse/LUCENE-969">LUCENE-969</a>
  for details.</p>
  <p>Typical Token reuse patterns:
  <ul>
  <li> Copying text from a string (type is reset to {@link #DEFAULT_TYPE} if not specified):<br/>
  <pre>
    return reusableToken.reinit(string, startOffset, endOffset[, type]);
  </pre>
  </li>
  <li> Copying some text from a string (type is reset to {@link #DEFAULT_TYPE} if not specified):<br/>
  <pre>
    return reusableToken.reinit(string, 0, string.length(), startOffset, endOffset[, type]);
  </pre>
  </li>
  </li>
  <li> Copying text from char[] buffer (type is reset to {@link #DEFAULT_TYPE} if not specified):<br/>
  <pre>
    return reusableToken.reinit(buffer, 0, buffer.length, startOffset, endOffset[, type]);
  </pre>
  </li>
  <li> Copying some text from a char[] buffer (type is reset to {@link #DEFAULT_TYPE} if not specified):<br/>
  <pre>
    return reusableToken.reinit(buffer, start, end - start, startOffset, endOffset[, type]);
  </pre>
  </li>
  <li> Copying from one one Token to another (type is reset to {@link #DEFAULT_TYPE} if not specified):<br/>
  <pre>
    return reusableToken.reinit(source.termBuffer(), 0, source.termLength(), source.startOffset(), source.endOffset()[, source.type()]);
  </pre>
  </li>
  </ul>
  A few things to note:
  <ul>
  <li>clear() initializes all of the fields to default values. This was changed in contrast to Lucene 2.4, but should affect no one.</li>
  <li>Because <code>TokenStreams</code> can be chained, one cannot assume that the <code>Token's</code> current type is correct.</li>
  <li>The startOffset and endOffset represent the start and offset in the source text, so be careful in adjusting them.</li>
  <li>When caching a reusable token, clone it. When injecting a cached token into a stream that can be reset, clone it again.</li>
  </ul>
  </p>

  @see org.apache.lucene.index.Payload
*/
public class Token extends AttributeImpl 
                   implements Cloneable, TermAttribute, TypeAttribute, PositionIncrementAttribute,
                              FlagsAttribute, OffsetAttribute, PayloadAttribute {

  public static final String DEFAULT_TYPE = "word";

  private static int MIN_BUFFER_SIZE = 10;

  /** @deprecated We will remove this when we remove the
   * deprecated APIs */
  private String termText;

  /**
   * Characters for the term text.
   * @deprecated This will be made private. Instead, use:
   * {@link #termBuffer()}, 
   * {@link #setTermBuffer(char[], int, int)},
   * {@link #setTermBuffer(String)}, or
   * {@link #setTermBuffer(String, int, int)}
   */
  char[] termBuffer;

  /**
   * Length of term text in the buffer.
   * @deprecated This will be made private. Instead, use:
   * {@link #termLength()}, or @{link setTermLength(int)}.
   */
  int termLength;

  /**
   * Start in source text.
   * @deprecated This will be made private. Instead, use:
   * {@link #startOffset()}, or @{link setStartOffset(int)}.
   */
  int startOffset;

  /**
   * End in source text.
   * @deprecated This will be made private. Instead, use:
   * {@link #endOffset()}, or @{link setEndOffset(int)}.
   */
  int endOffset;

  /**
   * The lexical type of the token.
   * @deprecated This will be made private. Instead, use:
   * {@link #type()}, or @{link setType(String)}.
   */
  String type = DEFAULT_TYPE;

  private int flags;
  
  /**
   * @deprecated This will be made private. Instead, use:
   * {@link #getPayload()}, or @{link setPayload(Payload)}.
   */
  Payload payload;
  
  /**
   * @deprecated This will be made private. Instead, use:
   * {@link #getPositionIncrement()}, or @{link setPositionIncrement(String)}.
   */
  int positionIncrement = 1;

  /** Constructs a Token will null text. */
  public Token() {
  }

  /** Constructs a Token with null text and start & end
   *  offsets.
   *  @param start start offset in the source text
   *  @param end end offset in the source text */
  public Token(int start, int end) {
    startOffset = start;
    endOffset = end;
  }

  /** Constructs a Token with null text and start & end
   *  offsets plus the Token type.
   *  @param start start offset in the source text
   *  @param end end offset in the source text
   *  @param typ the lexical type of this Token */
  public Token(int start, int end, String typ) {
    startOffset = start;
    endOffset = end;
    type = typ;
  }

  /**
   * Constructs a Token with null text and start & end
   *  offsets plus flags. NOTE: flags is EXPERIMENTAL.
   *  @param start start offset in the source text
   *  @param end end offset in the source text
   *  @param flags The bits to set for this token
   */
  public Token(int start, int end, int flags) {
    startOffset = start;
    endOffset = end;
    this.flags = flags;
  }

  /** Constructs a Token with the given term text, and start
   *  & end offsets.  The type defaults to "word."
   *  <b>NOTE:</b> for better indexing speed you should
   *  instead use the char[] termBuffer methods to set the
   *  term text.
   *  @param text term text
   *  @param start start offset
   *  @param end end offset
   */
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
   *  @param typ token type
   */
  public Token(String text, int start, int end, String typ) {
    termText = text;
    startOffset = start;
    endOffset = end;
    type = typ;
  }

  /**
   *  Constructs a Token with the given text, start and end
   *  offsets, & type.  <b>NOTE:</b> for better indexing
   *  speed you should instead use the char[] termBuffer
   *  methods to set the term text.
   * @param text
   * @param start
   * @param end
   * @param flags token type bits
   */
  public Token(String text, int start, int end, int flags) {
    termText = text;
    startOffset = start;
    endOffset = end;
    this.flags = flags;
  }

  /**
   *  Constructs a Token with the given term buffer (offset
   *  & length), start and end
   *  offsets
   * @param startTermBuffer
   * @param termBufferOffset
   * @param termBufferLength
   * @param start
   * @param end
   */
  public Token(char[] startTermBuffer, int termBufferOffset, int termBufferLength, int start, int end) {
    setTermBuffer(startTermBuffer, termBufferOffset, termBufferLength);
    startOffset = start;
    endOffset = end;
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
   * @param positionIncrement the distance from the prior term
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
   *  termBuffer methods to set the term text.
   *  @deprecated use {@link #setTermBuffer(char[], int, int)} or
   *                  {@link #setTermBuffer(String)} or
   *                  {@link #setTermBuffer(String, int, int)}.
   */
  public void setTermText(String text) {
    termText = text;
    termBuffer = null;
  }

  /** Returns the Token's term text.
   * 
   * @deprecated This method now has a performance penalty
   * because the text is stored internally in a char[].  If
   * possible, use {@link #termBuffer()} and {@link
   * #termLength()} directly instead.  If you really need a
   * String, use {@link #term()}</b>
   */
  public final String termText() {
    if (termText == null && termBuffer != null)
      termText = new String(termBuffer, 0, termLength);
    return termText;
  }

  /** Returns the Token's term text.
   * 
   * This method has a performance penalty
   * because the text is stored internally in a char[].  If
   * possible, use {@link #termBuffer()} and {@link
   * #termLength()} directly instead.  If you really need a
   * String, use this method, which is nothing more than
   * a convenience call to <b>new String(token.termBuffer(), 0, token.termLength())</b>
   */
  public final String term() {
    if (termText != null)
      return termText;
    initTermBuffer();
    return new String(termBuffer, 0, termLength);
  }

  /** Copies the contents of buffer, starting at offset for
   *  length characters, into the termBuffer array.
   *  @param buffer the buffer to copy
   *  @param offset the index in the buffer of the first character to copy
   *  @param length the number of characters to copy
   */
  public final void setTermBuffer(char[] buffer, int offset, int length) {
    termText = null;
    growTermBuffer(length);
    System.arraycopy(buffer, offset, termBuffer, 0, length);
    termLength = length;
  }

  /** Copies the contents of buffer into the termBuffer array.
   *  @param buffer the buffer to copy
   */
  public final void setTermBuffer(String buffer) {
    termText = null;
    final int length = buffer.length();
    growTermBuffer(length);
    buffer.getChars(0, length, termBuffer, 0);
    termLength = length;
  }

  /** Copies the contents of buffer, starting at offset and continuing
   *  for length characters, into the termBuffer array.
   *  @param buffer the buffer to copy
   *  @param offset the index in the buffer of the first character to copy
   *  @param length the number of characters to copy
   */
  public final void setTermBuffer(String buffer, int offset, int length) {
    assert offset <= buffer.length();
    assert offset + length <= buffer.length();
    termText = null;
    growTermBuffer(length);
    buffer.getChars(offset, offset + length, termBuffer, 0);
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

  /** Grows the termBuffer to at least size newSize, preserving the
   *  existing content. Note: If the next operation is to change
   *  the contents of the term buffer use
   *  {@link #setTermBuffer(char[], int, int)},
   *  {@link #setTermBuffer(String)}, or
   *  {@link #setTermBuffer(String, int, int)}
   *  to optimally combine the resize with the setting of the termBuffer.
   *  @param newSize minimum size of the new termBuffer
   *  @return newly created termBuffer with length >= newSize
   */
  public char[] resizeTermBuffer(int newSize) {
    if (termBuffer == null) {
      // The buffer is always at least MIN_BUFFER_SIZE
      newSize = newSize < MIN_BUFFER_SIZE ? MIN_BUFFER_SIZE : newSize;
      //Preserve termText 
      if (termText != null) {
        final int ttLen = termText.length();
        newSize = newSize < ttLen ? ttLen : newSize;
        termBuffer = new char[ArrayUtil.getNextSize(newSize)];
        termText.getChars(0, termText.length(), termBuffer, 0);
        termText = null;
      } else { // no term Text, the first allocation
        termBuffer = new char[ArrayUtil.getNextSize(newSize)];
      }    
    } else {
      if(termBuffer.length < newSize){
        // Not big enough; create a new array with slight
        // over allocation and preserve content
        final char[] newCharBuffer = new char[ArrayUtil.getNextSize(newSize)];
        System.arraycopy(termBuffer, 0, newCharBuffer, 0, termBuffer.length);
        termBuffer = newCharBuffer;
      }
    } 
    return termBuffer;   
  }

  /** Allocates a buffer char[] of at least newSize, without preserving the existing content.
   * its always used in places that set the content 
   *  @param newSize minimum size of the buffer
   */
  private void growTermBuffer(int newSize) {
    if (termBuffer == null) {
      // The buffer is always at least MIN_BUFFER_SIZE    
      termBuffer = new char[ArrayUtil.getNextSize(newSize < MIN_BUFFER_SIZE ? MIN_BUFFER_SIZE : newSize)];   
    } else {
      if(termBuffer.length < newSize){
        // Not big enough; create a new array with slight
        // over allocation:
        termBuffer = new char[ArrayUtil.getNextSize(newSize)];
      }
    } 
  }
  

  // TODO: once we remove the deprecated termText() method
  // and switch entirely to char[] termBuffer we don't need
  // to use this method anymore, only for late init of the buffer
  private void initTermBuffer() {
    if (termBuffer == null) {
      if (termText == null) {
        termBuffer = new char[ArrayUtil.getNextSize(MIN_BUFFER_SIZE)];
        termLength = 0;
      } else {
        int length = termText.length();
        if (length < MIN_BUFFER_SIZE) length = MIN_BUFFER_SIZE;
        termBuffer = new char[ArrayUtil.getNextSize(length)];
        termLength = termText.length();
        termText.getChars(0, termText.length(), termBuffer, 0);
        termText = null;
      }
    } else {
      termText = null;
    }
  }

  /** Return number of valid characters (length of the term)
   *  in the termBuffer array. */
  public final int termLength() {
    initTermBuffer();
    return termLength;
  }

  /** Set number of valid characters (length of the term) in
   *  the termBuffer array. Use this to truncate the termBuffer
   *  or to synchronize with external manipulation of the termBuffer.
   *  Note: to grow the size of the array,
   *  use {@link #resizeTermBuffer(int)} first.
   *  @param length the truncated length
   */
  public final void setTermLength(int length) {
    initTermBuffer();
    if (length > termBuffer.length)
      throw new IllegalArgumentException("length " + length + " exceeds the size of the termBuffer (" + termBuffer.length + ")");
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
    last character corresponding to this token in the source text. The length
    of the token in the source text is (endOffset - startOffset). */
  public final int endOffset() {
    return endOffset;
  }

  /** Set the ending offset.
      @see #endOffset() */
  public void setEndOffset(int offset) {
    this.endOffset = offset;
  }
  
  /** Set the starting and ending offset.
  @see #startOffset() and #endOffset()*/
  public void setOffset(int startOffset, int endOffset) {
    this.startOffset = startOffset;
    this.endOffset = endOffset;
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
   * EXPERIMENTAL:  While we think this is here to stay, we may want to change it to be a long.
   * <p/>
   *
   * Get the bitset for any bits that have been set.  This is completely distinct from {@link #type()}, although they do share similar purposes.
   * The flags can be used to encode information about the token for use by other {@link org.apache.lucene.analysis.TokenFilter}s.
   *
   *
   * @return The bits
   */
  public int getFlags() {
    return flags;
  }

  /**
   * @see #getFlags()
   */
  public void setFlags(int flags) {
    this.flags = flags;
  }

  /**
   * Returns this Token's payload.
   */ 
  public Payload getPayload() {
    return this.payload;
  }

  /** 
   * Sets this Token's payload.
   */
  public void setPayload(Payload payload) {
    this.payload = payload;
  }
  
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append('(');
    initTermBuffer();
    if (termBuffer == null)
      sb.append("null");
    else
      sb.append(termBuffer, 0, termLength);
      sb.append(',').append(startOffset).append(',').append(endOffset);
    if (!type.equals("word"))
      sb.append(",type=").append(type);
    if (positionIncrement != 1)
      sb.append(",posIncr=").append(positionIncrement);
    sb.append(')');
    return sb.toString();
  }

  /** Resets the term text, payload, flags, and positionIncrement,
   * startOffset, endOffset and token type to default.
   */
  public void clear() {
    payload = null;
    // Leave termBuffer to allow re-use
    termLength = 0;
    termText = null;
    positionIncrement = 1;
    flags = 0;
    startOffset = endOffset = 0;
    type = DEFAULT_TYPE;
  }

  public Object clone() {
    Token t = (Token)super.clone();
    // Do a deep clone
    if (termBuffer != null) {
      t.termBuffer = (char[]) termBuffer.clone();
    }
    if (payload != null) {
      t.payload = (Payload) payload.clone();
    }
    return t;
  }

  /** Makes a clone, but replaces the term buffer &
   * start/end offset in the process.  This is more
   * efficient than doing a full clone (and then calling
   * setTermBuffer) because it saves a wasted copy of the old
   * termBuffer. */
  public Token clone(char[] newTermBuffer, int newTermOffset, int newTermLength, int newStartOffset, int newEndOffset) {
    final Token t = new Token(newTermBuffer, newTermOffset, newTermLength, newStartOffset, newEndOffset);
    t.positionIncrement = positionIncrement;
    t.flags = flags;
    t.type = type;
    if (payload != null)
      t.payload = (Payload) payload.clone();
    return t;
  }

  public boolean equals(Object obj) {
    if (obj == this)
      return true;

    if (obj instanceof Token) {
      Token other = (Token) obj;

      initTermBuffer();
      other.initTermBuffer();
      
      if (termLength == other.termLength &&
          startOffset == other.startOffset &&
          endOffset == other.endOffset && 
          flags == other.flags &&
          positionIncrement == other.positionIncrement &&
          subEqual(type, other.type) &&
          subEqual(payload, other.payload)) {
        for(int i=0;i<termLength;i++)
          if (termBuffer[i] != other.termBuffer[i])
            return false;
        return true;
      } else
        return false;
    } else
      return false;
  }

  private boolean subEqual(Object o1, Object o2) {
    if (o1 == null)
      return o2 == null;
    else
      return o1.equals(o2);
  }

  public int hashCode() {
    initTermBuffer();
    int code = termLength;
    code = code * 31 + startOffset;
    code = code * 31 + endOffset;
    code = code * 31 + flags;
    code = code * 31 + positionIncrement;
    code = code * 31 + type.hashCode();
    code = (payload == null ? code : code * 31 + payload.hashCode());
    code = code * 31 + ArrayUtil.hashCode(termBuffer, 0, termLength);
    return code;
  }
      
  // like clear() but doesn't clear termBuffer/text
  private void clearNoTermBuffer() {
    payload = null;
    positionIncrement = 1;
    flags = 0;
    startOffset = endOffset = 0;
    type = DEFAULT_TYPE;
  }

  /** Shorthand for calling {@link #clear},
   *  {@link #setTermBuffer(char[], int, int)},
   *  {@link #setStartOffset},
   *  {@link #setEndOffset},
   *  {@link #setType}
   *  @return this Token instance */
  public Token reinit(char[] newTermBuffer, int newTermOffset, int newTermLength, int newStartOffset, int newEndOffset, String newType) {
    clearNoTermBuffer();
    payload = null;
    positionIncrement = 1;
    setTermBuffer(newTermBuffer, newTermOffset, newTermLength);
    startOffset = newStartOffset;
    endOffset = newEndOffset;
    type = newType;
    return this;
  }

  /** Shorthand for calling {@link #clear},
   *  {@link #setTermBuffer(char[], int, int)},
   *  {@link #setStartOffset},
   *  {@link #setEndOffset}
   *  {@link #setType} on Token.DEFAULT_TYPE
   *  @return this Token instance */
  public Token reinit(char[] newTermBuffer, int newTermOffset, int newTermLength, int newStartOffset, int newEndOffset) {
    clearNoTermBuffer();
    setTermBuffer(newTermBuffer, newTermOffset, newTermLength);
    startOffset = newStartOffset;
    endOffset = newEndOffset;
    type = DEFAULT_TYPE;
    return this;
  }

  /** Shorthand for calling {@link #clear},
   *  {@link #setTermBuffer(String)},
   *  {@link #setStartOffset},
   *  {@link #setEndOffset}
   *  {@link #setType}
   *  @return this Token instance */
  public Token reinit(String newTerm, int newStartOffset, int newEndOffset, String newType) {
    clearNoTermBuffer();
    setTermBuffer(newTerm);
    startOffset = newStartOffset;
    endOffset = newEndOffset;
    type = newType;
    return this;
  }

  /** Shorthand for calling {@link #clear},
   *  {@link #setTermBuffer(String, int, int)},
   *  {@link #setStartOffset},
   *  {@link #setEndOffset}
   *  {@link #setType}
   *  @return this Token instance */
  public Token reinit(String newTerm, int newTermOffset, int newTermLength, int newStartOffset, int newEndOffset, String newType) {
    clearNoTermBuffer();
    setTermBuffer(newTerm, newTermOffset, newTermLength);
    startOffset = newStartOffset;
    endOffset = newEndOffset;
    type = newType;
    return this;
  }

  /** Shorthand for calling {@link #clear},
   *  {@link #setTermBuffer(String)},
   *  {@link #setStartOffset},
   *  {@link #setEndOffset}
   *  {@link #setType} on Token.DEFAULT_TYPE
   *  @return this Token instance */
  public Token reinit(String newTerm, int newStartOffset, int newEndOffset) {
    clearNoTermBuffer();
    setTermBuffer(newTerm);
    startOffset = newStartOffset;
    endOffset = newEndOffset;
    type = DEFAULT_TYPE;
    return this;
  }

  /** Shorthand for calling {@link #clear},
   *  {@link #setTermBuffer(String, int, int)},
   *  {@link #setStartOffset},
   *  {@link #setEndOffset}
   *  {@link #setType} on Token.DEFAULT_TYPE
   *  @return this Token instance */
  public Token reinit(String newTerm, int newTermOffset, int newTermLength, int newStartOffset, int newEndOffset) {
    clearNoTermBuffer();
    setTermBuffer(newTerm, newTermOffset, newTermLength);
    startOffset = newStartOffset;
    endOffset = newEndOffset;
    type = DEFAULT_TYPE;
    return this;
  }

  /**
   * Copy the prototype token's fields into this one. Note: Payloads are shared.
   * @param prototype
   */
  public void reinit(Token prototype) {
    prototype.initTermBuffer();
    setTermBuffer(prototype.termBuffer, 0, prototype.termLength);
    positionIncrement = prototype.positionIncrement;
    flags = prototype.flags;
    startOffset = prototype.startOffset;
    endOffset = prototype.endOffset;
    type = prototype.type;
    payload =  prototype.payload;
  }

  /**
   * Copy the prototype token's fields into this one, with a different term. Note: Payloads are shared.
   * @param prototype
   * @param newTerm
   */
  public void reinit(Token prototype, String newTerm) {
    setTermBuffer(newTerm);
    positionIncrement = prototype.positionIncrement;
    flags = prototype.flags;
    startOffset = prototype.startOffset;
    endOffset = prototype.endOffset;
    type = prototype.type;
    payload =  prototype.payload;
  }

  /**
   * Copy the prototype token's fields into this one, with a different term. Note: Payloads are shared.
   * @param prototype
   * @param newTermBuffer
   * @param offset
   * @param length
   */
  public void reinit(Token prototype, char[] newTermBuffer, int offset, int length) {
    setTermBuffer(newTermBuffer, offset, length);
    positionIncrement = prototype.positionIncrement;
    flags = prototype.flags;
    startOffset = prototype.startOffset;
    endOffset = prototype.endOffset;
    type = prototype.type;
    payload =  prototype.payload;
  }

  public void copyTo(AttributeImpl target) {
    if (target instanceof Token) {
      final Token to = (Token) target;
      to.reinit(this);
      // reinit shares the payload, so clone it:
      if (payload !=null) {
        to.payload = (Payload) payload.clone();
      }
    // remove the following optimization in 3.0 when old TokenStream API removed:
    } else if (target instanceof TokenWrapper) {
      ((TokenWrapper) target).delegate = (Token) this.clone();
    } else {
      initTermBuffer();
      ((TermAttribute) target).setTermBuffer(termBuffer, 0, termLength);
      ((OffsetAttribute) target).setOffset(startOffset, endOffset);
      ((PositionIncrementAttribute) target).setPositionIncrement(positionIncrement);
      ((PayloadAttribute) target).setPayload((payload == null) ? null : (Payload) payload.clone());
      ((FlagsAttribute) target).setFlags(flags);
      ((TypeAttribute) target).setType(type);
    }
  }
}
