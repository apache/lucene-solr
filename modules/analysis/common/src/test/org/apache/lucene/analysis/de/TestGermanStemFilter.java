package org.apache.lucene.analysis.de;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.StringReader;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;

/**
 * Test the German stemmer. The stemming algorithm is known to work less 
 * than perfect, as it doesn't use any word lists with exceptions. We 
 * also check some of the cases where the algorithm is wrong.
 *
 */
public class TestGermanStemFilter extends BaseTokenStreamTestCase {

  public void testStemming() throws Exception {
    Tokenizer tokenizer = new KeywordTokenizer(new StringReader(""));
    TokenFilter filter = new GermanStemFilter(new LowerCaseFilter(TEST_VERSION_CURRENT, tokenizer));
    // read test cases from external file:
    InputStreamReader isr = new InputStreamReader(getClass().getResourceAsStream("data.txt"), "iso-8859-1");
    BufferedReader breader = new BufferedReader(isr);
    while(true) {
      String line = breader.readLine();
      if (line == null)
        break;
      line = line.trim();
      if (line.startsWith("#") || line.equals(""))
        continue;    // ignore comments and empty lines
      String[] parts = line.split(";");
      //System.out.println(parts[0] + " -- " + parts[1]);
      tokenizer.reset(new StringReader(parts[0]));
      filter.reset();
      assertTokenStreamContents(filter, new String[] { parts[1] });
    }
    breader.close();
    isr.close();
  }
}
