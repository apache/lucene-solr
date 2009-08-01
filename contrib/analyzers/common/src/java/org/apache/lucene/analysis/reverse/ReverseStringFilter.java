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
 * Reverse token string e.g. "country" => "yrtnuoc".
 *
 * @version $Id$
 */
public final class ReverseStringFilter extends TokenFilter {

  private TermAttribute termAtt;

  public ReverseStringFilter(TokenStream in) {
    super(in);
    termAtt = (TermAttribute) addAttribute(TermAttribute.class);
  }

  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      reverse( termAtt.termBuffer(), termAtt.termLength() );
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
