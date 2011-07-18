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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.util.Iterator;
import java.util.Arrays;

/** Simple tests to ensure this factory is working */
public class TestRemoveDuplicatesTokenFilterFactory extends BaseTokenTestCase {

  public static Token tok(int pos, String t, int start, int end) {
    Token tok = new Token(t,start,end);
    tok.setPositionIncrement(pos);
    return tok;
  }
  public static Token tok(int pos, String t) {
    return tok(pos, t, 0,0);
  }

  public void testDups(final String expected, final Token... tokens)
    throws Exception {

    final Iterator<Token> toks = Arrays.asList(tokens).iterator();
    RemoveDuplicatesTokenFilterFactory factory = new RemoveDuplicatesTokenFilterFactory();
    final TokenStream ts = factory.create
      (new TokenStream() {
          CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
          OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
          PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
          @Override
          public boolean incrementToken() {
            if (toks.hasNext()) {
              clearAttributes();
              Token tok = toks.next();
              termAtt.setEmpty().append(tok);
              offsetAtt.setOffset(tok.startOffset(), tok.endOffset());
              posIncAtt.setPositionIncrement(tok.getPositionIncrement());
              return true;
            } else {
              return false;
            }
          }
        });
    
    assertTokenStreamContents(ts, expected.split("\\s"));   
  }
 
  public void testSimpleDups() throws Exception {
    testDups("A B C D E"
             ,tok(1,"A", 0,  4)
             ,tok(1,"B", 5, 10)
             ,tok(0,"B",11, 15)
             ,tok(1,"C",16, 20)
             ,tok(0,"D",16, 20)
             ,tok(1,"E",21, 25)
             ); 
  }
}
