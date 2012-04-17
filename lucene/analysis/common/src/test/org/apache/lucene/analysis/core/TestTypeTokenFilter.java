package org.apache.lucene.analysis.core;

/*
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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.English;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.Set;


public class TestTypeTokenFilter extends BaseTokenStreamTestCase {

  public void testTypeFilter() throws IOException {
    StringReader reader = new StringReader("121 is palindrome, while 123 is not");
    Set<String> stopTypes = asSet("<NUM>");
    TokenStream stream = new TypeTokenFilter(true, new StandardTokenizer(TEST_VERSION_CURRENT, reader), stopTypes);
    assertTokenStreamContents(stream, new String[]{"is", "palindrome", "while", "is", "not"});
  }

  /**
   * Test Position increments applied by TypeTokenFilter with and without enabling this option.
   */
  public void testStopPositons() throws IOException {
    StringBuilder sb = new StringBuilder();
    for (int i = 10; i < 20; i++) {
      if (i % 3 != 0) {
        sb.append(i).append(" ");
      } else {
        String w = English.intToEnglish(i).trim();
        sb.append(w).append(" ");
      }
    }
    log(sb.toString());
    String stopTypes[] = new String[]{"<NUM>"};
    Set<String> stopSet = asSet(stopTypes);

    // with increments
    StringReader reader = new StringReader(sb.toString());
    TypeTokenFilter typeTokenFilter = new TypeTokenFilter(true, new StandardTokenizer(TEST_VERSION_CURRENT, reader), stopSet);
    testPositons(typeTokenFilter);

    // without increments
    reader = new StringReader(sb.toString());
    typeTokenFilter = new TypeTokenFilter(false, new StandardTokenizer(TEST_VERSION_CURRENT, reader), stopSet);
    testPositons(typeTokenFilter);

  }

  private void testPositons(TypeTokenFilter stpf) throws IOException {
    TypeAttribute typeAtt = stpf.getAttribute(TypeAttribute.class);
    CharTermAttribute termAttribute = stpf.getAttribute(CharTermAttribute.class);
    PositionIncrementAttribute posIncrAtt = stpf.getAttribute(PositionIncrementAttribute.class);
    stpf.reset();
    boolean enablePositionIncrements = stpf.getEnablePositionIncrements();
    while (stpf.incrementToken()) {
      log("Token: " + termAttribute.toString() + ": " + typeAtt.type() + " - " + posIncrAtt.getPositionIncrement());
      assertEquals("if position increment is enabled the positionIncrementAttribute value should be 3, otherwise 1",
          posIncrAtt.getPositionIncrement(), enablePositionIncrements ? 3 : 1);
    }
    stpf.end();
    stpf.close();
  }

  public void testTypeFilterWhitelist() throws IOException {
    StringReader reader = new StringReader("121 is palindrome, while 123 is not");
    Set<String> stopTypes = Collections.singleton("<NUM>");
    TokenStream stream = new TypeTokenFilter(true, new StandardTokenizer(TEST_VERSION_CURRENT, reader), stopTypes, true);
    assertTokenStreamContents(stream, new String[]{"121", "123"});
  }

  // print debug info depending on VERBOSE
  private static void log(String s) {
    if (VERBOSE) {
      System.out.println(s);
    }
  }
}
