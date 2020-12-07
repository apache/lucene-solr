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
package org.apache.lucene.analysis.ja;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ja.JapaneseTokenizer.Mode;

public class TestSearchMode extends BaseTokenStreamTestCase {
  private final static String SEGMENTATION_FILENAME = "search-segmentation-tests.txt";
  private Analyzer analyzer, analyzerNoOriginal;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new JapaneseTokenizer(newAttributeFactory(), null, true, false, Mode.SEARCH);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
    };
    analyzerNoOriginal = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new JapaneseTokenizer(newAttributeFactory(), null, true, true, Mode.SEARCH);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
    };
  }
  
  @Override
  public void tearDown() throws Exception {
    analyzer.close();
    analyzerNoOriginal.close();
    super.tearDown();
  }

  /** Test search mode segmentation */
  public void testSearchSegmentation() throws IOException {
    InputStream is = TestSearchMode.class.getResourceAsStream(SEGMENTATION_FILENAME);
    if (is == null) {
      throw new FileNotFoundException("Cannot find " + SEGMENTATION_FILENAME + " in test classpath");
    }
    try {
      LineNumberReader reader = new LineNumberReader(new InputStreamReader(is, StandardCharsets.UTF_8));
      String line = null;
      while ((line = reader.readLine()) != null) {
        // Remove comments
        line = line.replaceAll("#.*$", "");
        // Skip empty lines or comment lines
        if (line.trim().isEmpty()) {
          continue;
        }
        if (VERBOSE) {
          System.out.println("Line no. " + reader.getLineNumber() + ": " + line);
        }
        String[] fields = line.split("\t", 2);
        String sourceText = fields[0];
        String[] expectedTokens = fields[1].split("\\s+");
        int[] expectedPosIncrs = new int[expectedTokens.length];
        int[] expectedPosLengths = new int[expectedTokens.length];
        for(int tokIDX=0;tokIDX<expectedTokens.length;tokIDX++) {
          if (expectedTokens[tokIDX].endsWith("/0")) {
            expectedTokens[tokIDX] = expectedTokens[tokIDX].replace("/0", "");
            expectedPosLengths[tokIDX] = expectedTokens.length-1;
          } else {
            expectedPosIncrs[tokIDX] = 1;
            expectedPosLengths[tokIDX] = 1;
          }
        }
        assertAnalyzesTo(analyzer, sourceText, expectedTokens, expectedPosIncrs);
      }
    } finally {
      is.close();
    }
  }

  public void testSearchSegmentationNoOriginal() throws IOException {
    InputStream is = TestSearchMode.class.getResourceAsStream(SEGMENTATION_FILENAME);
    if (is == null) {
      throw new FileNotFoundException("Cannot find " + SEGMENTATION_FILENAME + " in test classpath");
    }
    try {
      LineNumberReader reader = new LineNumberReader(new InputStreamReader(is, StandardCharsets.UTF_8));
      String line = null;
      while ((line = reader.readLine()) != null) {
        // Remove comments
        line = line.replaceAll("#.*$", "");
        // Skip empty lines or comment lines
        if (line.trim().isEmpty()) {
          continue;
        }
        if (VERBOSE) {
          System.out.println("Line no. " + reader.getLineNumber() + ": " + line);
        }
        String[] fields = line.split("\t", 2);
        String sourceText = fields[0];
        String[] tmpExpectedTokens = fields[1].split("\\s+");

        List<String> expectedTokenList = new ArrayList<>();
        for(int tokIDX=0;tokIDX<tmpExpectedTokens.length;tokIDX++) {
          if (!tmpExpectedTokens[tokIDX].endsWith("/0")) {
            expectedTokenList.add(tmpExpectedTokens[tokIDX]);
          }
        }

        int[] expectedPosIncrs = new int[expectedTokenList.size()];
        int[] expectedPosLengths = new int[expectedTokenList.size()];
        for(int tokIDX=0;tokIDX<expectedTokenList.size();tokIDX++) {
          expectedPosIncrs[tokIDX] = 1;
          expectedPosLengths[tokIDX] = 1;
        }
        assertAnalyzesTo(analyzerNoOriginal, sourceText, expectedTokenList.toArray(new String[expectedTokenList.size()]), expectedPosIncrs);
      }
    } finally {
      is.close();
    }
  }
}
