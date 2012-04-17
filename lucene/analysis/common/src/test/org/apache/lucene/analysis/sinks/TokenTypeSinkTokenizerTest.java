package org.apache.lucene.analysis.sinks;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

public class TokenTypeSinkTokenizerTest extends BaseTokenStreamTestCase {

  public void test() throws IOException {
    TokenTypeSinkFilter sinkFilter = new TokenTypeSinkFilter("D");
    String test = "The quick red fox jumped over the lazy brown dogs";

    TeeSinkTokenFilter ttf = new TeeSinkTokenFilter(new WordTokenFilter(new MockTokenizer(new StringReader(test), MockTokenizer.WHITESPACE, false)));
    TeeSinkTokenFilter.SinkTokenStream sink = ttf.newSinkTokenStream(sinkFilter);
    
    boolean seenDogs = false;

    CharTermAttribute termAtt = ttf.addAttribute(CharTermAttribute.class);
    TypeAttribute typeAtt = ttf.addAttribute(TypeAttribute.class);
    ttf.reset();
    while (ttf.incrementToken()) {
      if (termAtt.toString().equals("dogs")) {
        seenDogs = true;
        assertTrue(typeAtt.type() + " is not equal to " + "D", typeAtt.type().equals("D") == true);
      } else {
        assertTrue(typeAtt.type() + " is not null and it should be", typeAtt.type().equals("word"));
      }
    }
    assertTrue(seenDogs + " does not equal: " + true, seenDogs == true);
    
    int sinkCount = 0;
    sink.reset();
    while (sink.incrementToken()) {
      sinkCount++;
    }

    assertTrue("sink Size: " + sinkCount + " is not: " + 1, sinkCount == 1);
  }

  private class WordTokenFilter extends TokenFilter {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    
    private WordTokenFilter(TokenStream input) {
      super(input);
    }

    @Override
    public final boolean incrementToken() throws IOException {
      if (!input.incrementToken()) return false;
      
      if (termAtt.toString().equals("dogs")) {
        typeAtt.setType("D");
      }
      return true;
    }
  }
}