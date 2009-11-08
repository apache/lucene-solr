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

package org.apache.lucene.analysis.reverse;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import java.io.IOException;

/**
 * Reverse token string, for example "country" => "yrtnuoc".
 * <p>
 * If <code>marker</code> is supplied, then tokens will be also prepended by
 * that character. For example, with a marker of &#x5C;u0001, "country" =>
 * "&#x5C;u0001yrtnuoc". This is useful when implementing efficient leading
 * wildcards search.
 * </p>
 */
public final class ReverseStringFilter extends TokenFilter {

  private TermAttribute termAtt;
  private final char marker;
  private static final char NOMARKER = '\uFFFF';
  
  /**
   * Example marker character: U+0001 (START OF HEADING) 
   */
  public static final char START_OF_HEADING_MARKER = '\u0001';
  
  /**
   * Example marker character: U+001F (INFORMATION SEPARATOR ONE)
   */
  public static final char INFORMATION_SEPARATOR_MARKER = '\u001F';
  
  /**
   * Example marker character: U+EC00 (PRIVATE USE AREA: EC00) 
   */
  public static final char PUA_EC00_MARKER = '\uEC00';
  
  /**
   * Example marker character: U+200F (RIGHT-TO-LEFT MARK)
   */
  public static final char RTL_DIRECTION_MARKER = '\u200F';
  
  /**
   * Create a new ReverseStringFilter that reverses all tokens in the 
   * supplied {@link TokenStream}.
   * <p>
   * The reversed tokens will not be marked. 
   * </p>
   * 
   * @param in {@link TokenStream} to filter
   */
  public ReverseStringFilter(TokenStream in) {
    this(in, NOMARKER);
  }

  /**
   * Create a new ReverseStringFilter that reverses and marks all tokens in the
   * supplied {@link TokenStream}.
   * <p>
   * The reversed tokens will be prepended (marked) by the <code>marker</code>
   * character.
   * </p>
   * 
   * @param in {@link TokenStream} to filter
   * @param marker A character used to mark reversed tokens
   */
  public ReverseStringFilter(TokenStream in, char marker) {
    super(in);
    this.marker = marker;
    termAtt = addAttribute(TermAttribute.class);
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      int len = termAtt.termLength();
      if (marker != NOMARKER) {
        len++;
        termAtt.resizeTermBuffer(len);
        termAtt.termBuffer()[len - 1] = marker;
      }
      reverse( termAtt.termBuffer(), len );
      termAtt.setTermLength(len);
      return true;
    } else {
      return false;
    }
  }

  public static String reverse( final String input ){
    char[] charInput = input.toCharArray();
    reverse( charInput );
    return new String( charInput );
  }
  
  public static void reverse( char[] buffer ){
    reverse( buffer, buffer.length );
  }
  
  public static void reverse( char[] buffer, int len ){
    reverse( buffer, 0, len );
  }
  
  public static void reverse( char[] buffer, int start, int len ){
    if( len <= 1 ) return;
    int num = len>>1;
    for( int i = start; i < ( start + num ); i++ ){
      char c = buffer[i];
      buffer[i] = buffer[start * 2 + len - i - 1];
      buffer[start * 2 + len - i - 1] = c;
    }
  }
}
