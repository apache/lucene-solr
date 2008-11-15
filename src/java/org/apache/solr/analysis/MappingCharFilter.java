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

package org.apache.solr.analysis;

import java.io.IOException;
import java.util.LinkedList;

/**
 *
 * @version $Id$
 * @since Solr 1.4
 *
 */
public class MappingCharFilter extends BaseCharFilter {

  private final NormalizeMap normMap;
  private LinkedList<Character> buffer;
  private String replacement;
  private int charPointer;
  private int nextCharCounter;

  public MappingCharFilter( NormalizeMap normMap, CharStream in ){
    super( in );
    this.normMap = normMap;
  }

  public int read() throws IOException {
    while( true ){
      if( replacement != null && charPointer < replacement.length() )
        return replacement.charAt( charPointer++ );

      int firstChar = nextChar();
      if( firstChar == -1 ) return -1;
      NormalizeMap nm = normMap.submap != null ?
        normMap.submap.get( (char)firstChar ) : null;
      if( nm == null ) return firstChar;
      NormalizeMap result = match( nm );
      if( result == null ) return firstChar;
      replacement = result.normStr;
      charPointer = 0;
      if( result.diff != 0 ){
        int prevCumulativeDiff = getLastCumulativeDiff();
        if( result.diff < 0 ){
          for( int i = 0; i < -result.diff ; i++ )
            addPosCorrectMap( nextCharCounter + i - prevCumulativeDiff, prevCumulativeDiff - 1 - i );
        }
        else{
          addPosCorrectMap( nextCharCounter - result.diff - prevCumulativeDiff, prevCumulativeDiff + result.diff ) ;
        }
      }
    }
  }

  private int nextChar() throws IOException {
    nextCharCounter++;
    if( buffer != null && !buffer.isEmpty() )
      return buffer.removeFirst();
    return input.read();
  }

  private void pushChar( int c ){
    nextCharCounter--;
    if( buffer == null )
      buffer = new LinkedList<Character>();
    buffer.addFirst( (char)c );
  }

  private void pushLastChar( int c ){
    if( buffer == null )
      buffer = new LinkedList<Character>();
    buffer.addLast( (char)c );
  }

  private NormalizeMap match( NormalizeMap map ) throws IOException {
    NormalizeMap result = null;
    if( map.submap != null ){
      int chr = nextChar();
      if( chr != -1 ){
        NormalizeMap subMap = map.submap.get( (char)chr );
        if( subMap != null ){
          result = match( subMap );
        }
        if( result == null )
          pushChar( chr );
      }
    }
    if( result == null && map.normStr != null )
      result = map;
    return result;
  }

  public int read( char[] cbuf, int off, int len ) throws IOException {
    char[] tmp = new char[len];
    int l = input.read( tmp, 0, len );
    if( l != -1 ){
      for( int i = 0; i < l; i++ )
        pushLastChar( tmp[i] );
    }
    l = 0;
    for( int i = off; i < off + len; i++ ){
      int c = read();
      if( c == -1 ) break;
      cbuf[i] = (char)c;
      l++;
    }
    return l == 0 ? -1 : l;
  }

  public boolean markSupported(){
    return false;
  }

  public void mark( int readAheadLimit ) throws IOException {
    throw new IOException( "mark/reset not supported" );
  }

  public void reset() throws IOException {
    throw new IOException( "mark/reset not supported" );
  }
}
