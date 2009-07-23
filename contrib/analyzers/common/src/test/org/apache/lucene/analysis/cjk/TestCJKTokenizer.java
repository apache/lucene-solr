package org.apache.lucene.analysis.cjk;

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

import java.io.IOException;
import java.io.StringReader;

import junit.framework.TestCase;
import org.apache.lucene.analysis.Token;


public class TestCJKTokenizer extends TestCase{

  public Token newToken(String termText, int start, int end, int type) {
    Token token = new Token(start, end);
    token.setTermBuffer(termText);
    token.setType(CJKTokenizer.TOKEN_TYPE_NAMES[type]);
    return token;
  }

  public void checkCJKToken(final String str, final Token[] out_tokens) throws IOException {
    CJKTokenizer tokenizer = new CJKTokenizer(new StringReader(str));
    int i = 0;
    System.out.println("string[" + str + "]");
    System.out.print("tokens[");
    final Token reusableToken = new Token();
    for (Token token = tokenizer.next(reusableToken) ;
         token != null                               ; 
         token = tokenizer.next(reusableToken)       ) {
      if (token.term().equals(out_tokens[i].term()) 
          && token.startOffset() == out_tokens[i].startOffset() 
          && token.endOffset() == out_tokens[i].endOffset() 
          && token.type().equals(out_tokens[i].type()) ) {
        System.out.print( token.term() + " ");
      }
      else {
        fail(token.term() + " (start: " + token.startOffset() 
             + " end: " + token.endOffset() + " type: " + token.type() + ") != "
             + out_tokens[i].term() + " (start: " + out_tokens[i].startOffset() 
             + " end: " + out_tokens[i].endOffset() 
             + " type: " + out_tokens[i].type() + ")");
        break;
      }
      ++i;
    }
    System.out.println("]" + System.getProperty("line.separator"));
  }
  
  public void testJa1() throws IOException {
    String str = "\u4e00\u4e8c\u4e09\u56db\u4e94\u516d\u4e03\u516b\u4e5d\u5341";
       
    Token[] out_tokens = { 
      newToken("\u4e00\u4e8c", 0, 2, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("\u4e8c\u4e09", 1, 3, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u4e09\u56db", 2, 4, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u56db\u4e94", 3, 5, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("\u4e94\u516d", 4, 6, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("\u516d\u4e03", 5, 7, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u4e03\u516b", 6, 8, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u516b\u4e5d", 7, 9, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u4e5d\u5341", 8,10, CJKTokenizer.DOUBLE_TOKEN_TYPE)
    };
    checkCJKToken(str, out_tokens);
  }
  
  public void testJa2() throws IOException {
    String str = "\u4e00 \u4e8c\u4e09\u56db \u4e94\u516d\u4e03\u516b\u4e5d \u5341";
       
    Token[] out_tokens = { 
      newToken("\u4e00", 0, 1, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("\u4e8c\u4e09", 2, 4, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u4e09\u56db", 3, 5, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u4e94\u516d", 6, 8, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("\u516d\u4e03", 7, 9, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u4e03\u516b", 8, 10, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u516b\u4e5d", 9, 11, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u5341", 12,13, CJKTokenizer.DOUBLE_TOKEN_TYPE)
    };
    checkCJKToken(str, out_tokens);
  }
  
  public void testC() throws IOException {
    String str = "abc defgh ijklmn opqrstu vwxy z";
       
    Token[] out_tokens = { 
      newToken("abc", 0, 3, CJKTokenizer.SINGLE_TOKEN_TYPE), 
      newToken("defgh", 4, 9, CJKTokenizer.SINGLE_TOKEN_TYPE),
      newToken("ijklmn", 10, 16, CJKTokenizer.SINGLE_TOKEN_TYPE),
      newToken("opqrstu", 17, 24, CJKTokenizer.SINGLE_TOKEN_TYPE), 
      newToken("vwxy", 25, 29, CJKTokenizer.SINGLE_TOKEN_TYPE), 
      newToken("z", 30, 31, CJKTokenizer.SINGLE_TOKEN_TYPE),
    };
    checkCJKToken(str, out_tokens);
  }
  
  public void testMix() throws IOException {
    String str = "\u3042\u3044\u3046\u3048\u304aabc\u304b\u304d\u304f\u3051\u3053";
       
    Token[] out_tokens = { 
      newToken("\u3042\u3044", 0, 2, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("\u3044\u3046", 1, 3, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u3046\u3048", 2, 4, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u3048\u304a", 3, 5, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("abc", 5, 8, CJKTokenizer.SINGLE_TOKEN_TYPE), 
      newToken("\u304b\u304d", 8, 10, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u304d\u304f", 9, 11, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u304f\u3051", 10,12, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u3051\u3053", 11,13, CJKTokenizer.DOUBLE_TOKEN_TYPE)
    };
    checkCJKToken(str, out_tokens);
  }
  
  public void testMix2() throws IOException {
    String str = "\u3042\u3044\u3046\u3048\u304aab\u3093c\u304b\u304d\u304f\u3051 \u3053";
       
    Token[] out_tokens = { 
      newToken("\u3042\u3044", 0, 2, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("\u3044\u3046", 1, 3, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u3046\u3048", 2, 4, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u3048\u304a", 3, 5, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("ab", 5, 7, CJKTokenizer.SINGLE_TOKEN_TYPE), 
      newToken("\u3093", 7, 8, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("c", 8, 9, CJKTokenizer.SINGLE_TOKEN_TYPE), 
      newToken("\u304b\u304d", 9, 11, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u304d\u304f", 10, 12, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u304f\u3051", 11,13, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u3053", 14,15, CJKTokenizer.DOUBLE_TOKEN_TYPE)
    };
    checkCJKToken(str, out_tokens);
  }

  public void testSingleChar() throws IOException {
    String str = "\u4e00";
       
    Token[] out_tokens = { 
      newToken("\u4e00", 0, 1, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
    };
    checkCJKToken(str, out_tokens);
  }
}
