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
import java.util.Iterator;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;


/**
 * @version $Id:$
 */
public class TestTrimFilter extends TestCase {
  
  public void testTrim() throws Exception {
    TokenStream ts = new TrimFilter
      (new IterTokenStream(new Token(" a ", 1, 5),
                           new Token("b   ",6,10),
                           new Token("cCc",11,15),
                           new Token("   ",16,20)));

    assertEquals("a", ts.next().termText());
    assertEquals("b", ts.next().termText());
    assertEquals("cCc", ts.next().termText());
    assertEquals("", ts.next().termText());
    assertNull(ts.next());
  }

  public static class IterTokenStream extends TokenStream {
    Iterator<Token> toks;
    public IterTokenStream(Token... toks) {
      this.toks = Arrays.asList(toks).iterator();
    }
    public Token next() {
      if (toks.hasNext()) {
        return toks.next();
      }
      return null;
    }
  }
}
