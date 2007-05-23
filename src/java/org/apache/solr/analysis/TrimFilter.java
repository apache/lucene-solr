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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;

import java.io.IOException;

/**
 * Trims leading and trailing whitespace from Tokens in the stream.
 *
 * @version $Id:$
 */
public final class TrimFilter extends TokenFilter {
  
  final boolean updateOffsets;

  public TrimFilter(TokenStream in, boolean updateOffsets ) {
    super(in);
    this.updateOffsets = updateOffsets;
  }

  @Override
  public final Token next() throws IOException {
    Token t = input.next();
    if (null == t || null == t.termText())
      return t;

    if( updateOffsets ) {
      String txt = t.termText();
      int start = 0;
      int end = txt.length();
      int endOff = 0;
      
      // eat the first characters
      while ((start < end) && (txt.charAt(start) <= ' ')) {
        start++;
      }
      
      // eat the end characters
      while ((start < end) && (txt.charAt(end-1) <= ' ')) {
        end--;
        endOff++;
      }
      
      if( start > 0 || end < txt.length() ) {
        int incr = t.getPositionIncrement();
        t = new Token( t.termText().substring( start, end ),
             t.startOffset()+start,
             t.endOffset()-endOff,
             t.type() );
        
        t.setPositionIncrement( incr ); //+ start ); TODO? what should happen with the offset
      }
    }
    else {
      t.setTermText( t.termText().trim() );
    }
    return t;
  }
}
