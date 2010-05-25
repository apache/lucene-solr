package org.apache.lucene.analysis.en;

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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.zip.ZipFile;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.miscellaneous.KeywordMarkerFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

/**
 * Test the PorterStemFilter with Martin Porter's test data.
 */
public class TestPorterStemFilter extends BaseTokenStreamTestCase {  
  /**
   * Run the stemmer against all strings in voc.txt
   * The output should be the same as the string in output.txt
   */
  public void testPorterStemFilter() throws Exception {
    Tokenizer tokenizer = new KeywordTokenizer(new StringReader(""));
    TokenStream filter = new PorterStemFilter(tokenizer);   
    ZipFile zipFile = new ZipFile(getDataFile("porterTestData.zip"));
    InputStream voc = zipFile.getInputStream(zipFile.getEntry("voc.txt"));
    InputStream out = zipFile.getInputStream(zipFile.getEntry("output.txt"));
    BufferedReader vocReader = new BufferedReader(new InputStreamReader(
        voc, "UTF-8"));
    BufferedReader outputReader = new BufferedReader(new InputStreamReader(
        out, "UTF-8"));
    String inputWord = null;
    while ((inputWord = vocReader.readLine()) != null) {
      String expectedWord = outputReader.readLine();
      assertNotNull(expectedWord);
      tokenizer.reset(new StringReader(inputWord));
      filter.reset();
      assertTokenStreamContents(filter, new String[] { expectedWord });
    }
    vocReader.close();
    outputReader.close();
    zipFile.close();
  }
  
  public void testWithKeywordAttribute() throws IOException {
    CharArraySet set = new CharArraySet(TEST_VERSION_CURRENT, 1, true);
    set.add("yourselves");
    Tokenizer tokenizer = new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader("yourselves yours"));
    TokenStream filter = new PorterStemFilter(new KeywordMarkerFilter(tokenizer, set));   
    assertTokenStreamContents(filter, new String[] {"yourselves", "your"});
  }
}
