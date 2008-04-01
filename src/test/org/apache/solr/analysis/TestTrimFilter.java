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

import java.util.List;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;


/**
 * @version $Id:$
 */
public class TestTrimFilter extends BaseTokenTestCase {
  
  public void testTrim() throws Exception {
    TokenStream ts = new TrimFilter
      (new IterTokenStream(new Token(" a ", 1, 5),
                           new Token("b   ",6,10),
                           new Token("cCc",11,15),
                           new Token("   ",16,20)), false );

    Token token = ts.next();
    assertEquals("a", new String(token.termBuffer(), 0, token.termLength()));
    token = ts.next();
    assertEquals("b", new String(token.termBuffer(), 0, token.termLength()));
    token = ts.next();
    assertEquals("cCc", new String(token.termBuffer(), 0, token.termLength()));
    token = ts.next();
    assertEquals("", new String(token.termBuffer(), 0, token.termLength()));
    token = ts.next();
    assertNull(token);
    
    ts = new TrimFilter( new IterTokenStream(
           new Token(" a", 0,2),
           new Token("b ", 0,2),
           new Token(" c ",0,3),
           new Token("   ",0,3)), true );
    
    List<Token> expect = tokens( "a,1,1,2 b,1,0,1 c,1,1,2 ,1,3,3" );
    List<Token> real = getTokens(ts);
    for( Token t : expect ) {
      System.out.println( "TEST:" + t );
    }
    for( Token t : real ) {
      System.out.println( "REAL:" + t );
    }
    assertTokEqualOff( expect, real );
  }

}
