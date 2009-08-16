package org.apache.lucene.analysis.ngram;

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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import java.io.IOException;
import java.io.StringReader;

import junit.framework.TestCase;

/**
 * Tests {@link NGramTokenFilter} for correctness.
 */
public class NGramTokenFilterTest extends TestCase {
    private TokenStream input;
    
    public void setUp() {
        input = new WhitespaceTokenizer(new StringReader("abcde"));
    }

    public void testInvalidInput() throws Exception {
        boolean gotException = false;
        try {        
            new NGramTokenFilter(input, 2, 1);
        } catch (IllegalArgumentException e) {
            gotException = true;
        }
        assertTrue(gotException);
    }

    public void testInvalidInput2() throws Exception {
        boolean gotException = false;
        try {        
            new NGramTokenFilter(input, 0, 1);
        } catch (IllegalArgumentException e) {
            gotException = true;
        }
        assertTrue(gotException);
    }

    private void checkStream(TokenStream stream, String[] exp) throws IOException {
      TermAttribute termAtt = (TermAttribute) stream.addAttribute(TermAttribute.class);
      for (int i = 0; i < exp.length; i++) {
        assertTrue(stream.incrementToken());
        assertEquals(exp[i], termAtt.toString());
      }
      assertFalse(stream.incrementToken());
    }
    
    public void testUnigrams() throws Exception {
      NGramTokenFilter filter = new NGramTokenFilter(input, 1, 1);
      String[] exp = new String[] {
        "(a,0,1)", "(b,1,2)", "(c,2,3)", "(d,3,4)", "(e,4,5)"
      };
      
      checkStream(filter, exp);
    }

    public void testBigrams() throws Exception {
      NGramTokenFilter filter = new NGramTokenFilter(input, 2, 2);
      String[] exp = new String[] {
          "(ab,0,2)", "(bc,1,3)", "(cd,2,4)", "(de,3,5)"
        };
        
      checkStream(filter, exp);
    }

    public void testNgrams() throws Exception {
      NGramTokenFilter filter = new NGramTokenFilter(input, 1, 3);
      String[] exp = new String[] {
          "(a,0,1)", "(b,1,2)", "(c,2,3)", "(d,3,4)", "(e,4,5)",
          "(ab,0,2)", "(bc,1,3)", "(cd,2,4)", "(de,3,5)",
          "(abc,0,3)", "(bcd,1,4)", "(cde,2,5)"
      };
        
      checkStream(filter, exp);
    }

    public void testOversizedNgrams() throws Exception {
      NGramTokenFilter filter = new NGramTokenFilter(input, 6, 7);
      assertFalse(filter.incrementToken());
    }
    
    public void testSmallTokenInStream() throws Exception {
      input = new WhitespaceTokenizer(new StringReader("abc de fgh"));
      NGramTokenFilter filter = new NGramTokenFilter(input, 3, 3);
      String[] exp = new String[] {
          "(abc,0,3)", "(fgh,0,3)"
        };
        
      checkStream(filter, exp);
    }
    
    public void testReset() throws Exception {
      WhitespaceTokenizer tokenizer = new WhitespaceTokenizer(new StringReader("abcde"));
      NGramTokenFilter filter = new NGramTokenFilter(tokenizer, 1, 3);
      TermAttribute termAtt = (TermAttribute) filter.addAttribute(TermAttribute.class);
      assertTrue(filter.incrementToken());
      assertEquals("(a,0,1)", termAtt.toString());
      assertTrue(filter.incrementToken());
      assertEquals("(b,1,2)", termAtt.toString());
      tokenizer.reset(new StringReader("abcde"));
      filter.reset();
      assertTrue(filter.incrementToken());
      assertEquals("(a,0,1)", termAtt.toString());
    }
}
