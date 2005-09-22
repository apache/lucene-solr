package org.apache.lucene.analysis.snowball;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2004 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.io.*;

import junit.framework.*;

import org.apache.lucene.analysis.*;

public class TestSnowball extends TestCase {

  public void assertAnalyzesTo(Analyzer a,
                               String input,
                               String[] output) throws Exception {
    TokenStream ts = a.tokenStream("dummy", new StringReader(input));
    for (int i = 0; i < output.length; i++) {
      Token t = ts.next();
      assertNotNull(t);
      assertEquals(output[i], t.termText());
    }
    assertNull(ts.next());
    ts.close();
  }

  public void testEnglish() throws Exception {
    Analyzer a = new SnowballAnalyzer("English");
    assertAnalyzesTo(a, "he abhorred accents",
        new String[]{"he", "abhor", "accent"});
  }


  public void testFilterTokens() throws Exception {
    final Token tok = new Token("accents", 2, 7, "wrd");
    tok.setPositionIncrement(3);

    SnowballFilter filter = new SnowballFilter(
        new TokenStream() {
          public Token next() {
            return tok;
          }
        },
        "English"
    );

    Token newtok = filter.next();

    assertEquals("accent", newtok.termText());
    assertEquals(2, newtok.startOffset());
    assertEquals(7, newtok.endOffset());
    assertEquals("wrd", newtok.type());
    assertEquals(3, newtok.getPositionIncrement());
  }
}

