package org.apache.lucene.analysis.fr;

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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * 
 */
public class TestElision extends BaseTokenStreamTestCase {

  public void testElision() throws Exception {
    String test = "Plop, juste pour voir l'embrouille avec O'brian. M'enfin.";
    Tokenizer tokenizer = new StandardTokenizer(TEST_VERSION_CURRENT, new StringReader(test));
    Set<String> articles = new HashSet<String>();
    articles.add("l");
    articles.add("M");
    TokenFilter filter = new ElisionFilter(TEST_VERSION_CURRENT, tokenizer, articles);
    List<String> tas = filter(filter);
    assertEquals("embrouille", tas.get(4));
    assertEquals("O'brian", tas.get(6));
    assertEquals("enfin", tas.get(7));
  }

  private List<String> filter(TokenFilter filter) throws IOException {
    List<String> tas = new ArrayList<String>();
    CharTermAttribute termAtt = filter.getAttribute(CharTermAttribute.class);
    while (filter.incrementToken()) {
      tas.add(termAtt.toString());
    }
    return tas;
  }

}
