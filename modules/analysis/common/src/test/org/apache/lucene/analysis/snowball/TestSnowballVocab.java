package org.apache.lucene.analysis.snowball;

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
import org.apache.lucene.analysis.KeywordTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

/**
 * Test the snowball filters against the snowball data tests
 */
public class TestSnowballVocab extends BaseTokenStreamTestCase {
  private Tokenizer tokenizer = new KeywordTokenizer(new StringReader(""));
  ZipFile zipFile = null;
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    this.zipFile = new ZipFile(getDataFile("TestSnowballVocabData.zip"));
  }
  
  @Override
  protected void tearDown() throws Exception {
    this.zipFile.close();
    this.zipFile = null;
    super.tearDown();
  }

  /**
   * Run all languages against their snowball vocabulary tests.
   */
  public void testStemmers() throws IOException {
    assertCorrectOutput("Danish", "danish");
    assertCorrectOutput("Dutch", "dutch");
    assertCorrectOutput("English", "english");
    // disabled due to snowball java code generation bug: 
    // see http://article.gmane.org/gmane.comp.search.snowball/1139
    // assertCorrectOutput("Finnish", "finnish");
    assertCorrectOutput("French", "french");
    assertCorrectOutput("German", "german");
    assertCorrectOutput("German2", "german2");
    assertCorrectOutput("Hungarian", "hungarian");
    assertCorrectOutput("Italian", "italian");
    assertCorrectOutput("Kp", "kraaij_pohlmann");
    // disabled due to snowball java code generation bug: 
    // see http://article.gmane.org/gmane.comp.search.snowball/1139
    // assertCorrectOutput("Lovins", "lovins");
    assertCorrectOutput("Norwegian", "norwegian");
    assertCorrectOutput("Porter", "porter");
    assertCorrectOutput("Portuguese", "portuguese");
    assertCorrectOutput("Romanian", "romanian");
    assertCorrectOutput("Russian", "russian");
    assertCorrectOutput("Spanish", "spanish");
    assertCorrectOutput("Swedish", "swedish");
    assertCorrectOutput("Turkish", "turkish");
  }
    
  /**
   * For the supplied language, run the stemmer against all strings in voc.txt
   * The output should be the same as the string in output.txt
   */
  private void assertCorrectOutput(String snowballLanguage, String dataDirectory)
      throws IOException {
    if (VERBOSE) System.out.println("checking snowball language: " + snowballLanguage);
    TokenStream filter = new SnowballFilter(tokenizer, snowballLanguage);
    InputStream voc = zipFile.getInputStream(zipFile.getEntry(dataDirectory + "/voc.txt"));
    InputStream out = zipFile.getInputStream(zipFile.getEntry(dataDirectory + "/output.txt"));
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
      assertTokenStreamContents(filter, new String[] {expectedWord});
    }
    vocReader.close();
    outputReader.close();
  }
}
