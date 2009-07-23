package org.apache.lucene.analysis.br;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

/**
 * Test the Brazilian Stem Filter, which only modifies the term text.
 * 
 * It is very similar to the snowball portuguese algorithm but not exactly the same.
 *
 */
public class TestBrazilianStemmer extends TestCase {
  
  public void testWithSnowballExamples() throws IOException {
	 check("boa", "boa");
	 check("boainain", "boainain");
	 check("boas", "boas");
	 check("bôas", "boas"); // removes diacritic: different from snowball portugese
	 check("boassu", "boassu");
	 check("boataria", "boat");
	 check("boate", "boat");
	 check("boates", "boat");
	 check("boatos", "boat");
	 check("bob", "bob");
	 check("boba", "bob");
	 check("bobagem", "bobag");
	 check("bobagens", "bobagens");
	 check("bobalhões", "bobalho"); // removes diacritic: different from snowball portugese
	 check("bobear", "bob");
	 check("bobeira", "bobeir");
	 check("bobinho", "bobinh");
	 check("bobinhos", "bobinh");
	 check("bobo", "bob");
	 check("bobs", "bobs");
	 check("boca", "boc");
	 check("bocadas", "boc");
	 check("bocadinho", "bocadinh");
	 check("bocado", "boc");
	 check("bocaiúva", "bocaiuv"); // removes diacritic: different from snowball portuguese
	 check("boçal", "bocal"); // removes diacritic: different from snowball portuguese
	 check("bocarra", "bocarr");
	 check("bocas", "boc");
	 check("bode", "bod");
	 check("bodoque", "bodoqu");
	 check("body", "body");
	 check("boeing", "boeing");
	 check("boem", "boem");
	 check("boemia", "boem");
	 check("boêmio", "boemi"); // removes diacritic: different from snowball portuguese
	 check("bogotá", "bogot");
	 check("boi", "boi");
	 check("bóia", "boi"); // removes diacritic: different from snowball portuguese
	 check("boiando", "boi");
	 check("quiabo", "quiab");
	 check("quicaram", "quic");
	 check("quickly", "quickly");
	 check("quieto", "quiet");
	 check("quietos", "quiet");
	 check("quilate", "quilat");
	 check("quilates", "quilat");
	 check("quilinhos", "quilinh");
	 check("quilo", "quil");
	 check("quilombo", "quilomb");
	 check("quilométricas", "quilometr"); // removes diacritic: different from snowball portuguese
	 check("quilométricos", "quilometr"); // removes diacritic: different from snowball portuguese
	 check("quilômetro", "quilometr"); // removes diacritic: different from snowball portoguese
	 check("quilômetros", "quilometr"); // removes diacritic: different from snowball portoguese
	 check("quilos", "quil");
	 check("quimica", "quimic");
	 check("quilos", "quil");
	 check("quimica", "quimic");
	 check("quimicas", "quimic");
	 check("quimico", "quimic");
	 check("quimicos", "quimic");
	 check("quimioterapia", "quimioterap");
	 check("quimioterápicos", "quimioterap"); // removes diacritic: different from snowball portoguese
	 check("quimono", "quimon");
	 check("quincas", "quinc");
	 check("quinhão", "quinha"); // removes diacritic: different from snowball portoguese
	 check("quinhentos", "quinhent");
	 check("quinn", "quinn");
	 check("quino", "quin");
	 check("quinta", "quint");
	 check("quintal", "quintal");
	 check("quintana", "quintan");
	 check("quintanilha", "quintanilh");
	 check("quintão", "quinta"); // removes diacritic: different from snowball portoguese
	 check("quintessência", "quintessente"); // versus snowball portuguese 'quintessent'
	 check("quintino", "quintin");
	 check("quinto", "quint");
	 check("quintos", "quint");
	 check("quintuplicou", "quintuplic");
	 check("quinze", "quinz");
	 check("quinzena", "quinzen");
	 check("quiosque", "quiosqu");
  }
 

  private void check(final String input, final String expected) throws IOException {
    Analyzer analyzer = new BrazilianAnalyzer(); 
    TokenStream stream = analyzer.tokenStream("dummy", new StringReader(input));
    final Token reusableToken = new Token();
    Token nextToken = stream.next(reusableToken);
    if (nextToken == null)
      fail();
    assertEquals(expected, nextToken.term());
    assertTrue(stream.next(nextToken) == null);
    stream.close();
  }

}