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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.BaseCharFilter;
import org.apache.lucene.analysis.CharStream;

/**
 * CharFilter that uses a regular expression for the target of replace string.
 * The pattern match will be done in each "block" in char stream.
 * 
 * <p>
 * ex1) source="aa&nbsp;&nbsp;bb&nbsp;aa&nbsp;bb", pattern="(aa)\\s+(bb)" replacement="$1#$2"<br/>
 * output="aa#bb&nbsp;aa#bb"
 * </p>
 * 
 * NOTE: If you produce a phrase that has different length to source string
 * and the field is used for highlighting for a term of the phrase, you will
 * face a trouble.
 * 
 * <p>
 * ex2) source="aa123bb", pattern="(aa)\\d+(bb)" replacement="$1&nbsp;$2"<br/>
 * output="aa&nbsp;bb"<br/>
 * and you want to search bb and highlight it, you will get<br/>
 * highlight snippet="aa1&lt;em&gt;23bb&lt;/em&gt;"
 * </p>
 * 
 * @version $Id$
 * @since Solr 1.5
 */
public class PatternReplaceCharFilter extends BaseCharFilter {

  private final Pattern pattern;
  private final String replacement;
  private final int maxBlockChars;
  private final String blockDelimiters;
  public static final int DEFAULT_MAX_BLOCK_CHARS = 10000;

  private LinkedList<Character> buffer;
  private int nextCharCounter;
  private char[] blockBuffer;
  private int blockBufferLength;
  private String replaceBlockBuffer;
  private int replaceBlockBufferOffset;
  
  public PatternReplaceCharFilter( Pattern pattern, String replacement, CharStream in ){
    this( pattern, replacement, DEFAULT_MAX_BLOCK_CHARS, null, in );
  }

  public PatternReplaceCharFilter( Pattern pattern, String replacement,
      int maxBlockChars, CharStream in ){
    this( pattern, replacement, maxBlockChars, null, in );
  }

  public PatternReplaceCharFilter( Pattern pattern, String replacement,
      String blockDelimiters, CharStream in ){
    this( pattern, replacement, DEFAULT_MAX_BLOCK_CHARS, blockDelimiters, in );
  }

  public PatternReplaceCharFilter( Pattern pattern, String replacement,
      int maxBlockChars, String blockDelimiters, CharStream in ){
    super( in );
    this.pattern = pattern;
    this.replacement = replacement;
    if( maxBlockChars < 1 )
      throw new IllegalArgumentException( "maxBlockChars should be greater than 0, but it is " + maxBlockChars );
    this.maxBlockChars = maxBlockChars;
    this.blockDelimiters = blockDelimiters;
    blockBuffer = new char[maxBlockChars];
  }
  
  private boolean prepareReplaceBlock() throws IOException {
    while( true ){
      if( replaceBlockBuffer != null && replaceBlockBuffer.length() > replaceBlockBufferOffset )
        return true;
      // prepare block buffer
      blockBufferLength = 0;
      while( true ){
        int c = nextChar();
        if( c == -1 ) break;
        blockBuffer[blockBufferLength++] = (char)c;
        // end of block?
        boolean foundDelimiter =
          ( blockDelimiters != null ) &&
          ( blockDelimiters.length() > 0 ) &&
          blockDelimiters.indexOf( c ) >= 0;
        if( foundDelimiter ||
            blockBufferLength >= maxBlockChars ) break;
      }
      // block buffer available?
      if( blockBufferLength == 0 ) return false;
      replaceBlockBuffer = getReplaceBlock( blockBuffer, 0, blockBufferLength );
      replaceBlockBufferOffset = 0;
    }
  }

  @Override
  public int read() throws IOException {
    while( prepareReplaceBlock() ){
      return replaceBlockBuffer.charAt( replaceBlockBufferOffset++ );
    }
    return -1;
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    char[] tmp = new char[len];
    int l = input.read(tmp, 0, len);
    if (l != -1) {
      for(int i = 0; i < l; i++)
        pushLastChar(tmp[i]);
    }
    l = 0;
    for(int i = off; i < off + len; i++) {
      int c = read();
      if (c == -1) break;
      cbuf[i] = (char) c;
      l++;
    }
    return l == 0 ? -1 : l;
  }

  private int nextChar() throws IOException {
    if (buffer != null && !buffer.isEmpty()) {
      nextCharCounter++;
      return buffer.removeFirst().charValue();
    }
    int c = input.read();
    if( c != -1 )
      nextCharCounter++;
    return c;
  }

  private void pushLastChar(int c) {
    if (buffer == null) {
      buffer = new LinkedList<Character>();
    }
    buffer.addLast(new Character((char) c));
  }
  
  String getReplaceBlock( String block ){
    char[] blockChars = block.toCharArray();
    return getReplaceBlock( blockChars, 0, blockChars.length );
  }
    
  String getReplaceBlock( char block[], int offset, int length ){
    StringBuffer replaceBlock = new StringBuffer();
    String sourceBlock = new String( block, offset, length );
    Matcher m = pattern.matcher( sourceBlock );
    int lastMatchOffset = 0, lastDiff = 0;
    while( m.find() ){
      m.appendReplacement( replaceBlock, replacement );
      // record cumulative diff for the offset correction
      int diff = replaceBlock.length() - lastMatchOffset - lastDiff - ( m.end( 0 ) - lastMatchOffset );
      if (diff != 0) {
        int prevCumulativeDiff = getLastCumulativeDiff();
        if (diff > 0) {
          for(int i = 0; i < diff; i++){
            addOffCorrectMap(nextCharCounter - length + m.end( 0 ) + i - prevCumulativeDiff,
                prevCumulativeDiff - 1 - i);
          }
        } else {
          addOffCorrectMap(nextCharCounter - length + m.end( 0 ) + diff - prevCumulativeDiff,
              prevCumulativeDiff - diff);
        }
      }
      // save last offsets
      lastMatchOffset = m.end( 0 );
      lastDiff = diff;
    }
    // copy remaining of the part of source block
    m.appendTail( replaceBlock );
    return replaceBlock.toString();
  }
}
