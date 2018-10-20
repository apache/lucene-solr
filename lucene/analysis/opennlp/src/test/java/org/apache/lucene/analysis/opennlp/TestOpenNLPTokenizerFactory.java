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

package org.apache.lucene.analysis.opennlp;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.util.ClasspathResourceLoader;
import org.junit.Test;

/**
 * Tests the Tokenizer as well- the Tokenizer needs the OpenNLP model files,
 * which this can load from src/test-files/opennlp/solr/conf
 *
 */
public class TestOpenNLPTokenizerFactory extends BaseTokenStreamTestCase {

  static private String SENTENCES = "Sentence number 1 has 6 words. Sentence number 2, 5 words.";
  static private String[] SENTENCES_punc = {"Sentence", "number", "1", "has", "6", "words", ".", "Sentence", "number", "2", ",", "5", "words", "."};
  static private int[] SENTENCES_startOffsets = {0, 9, 16, 18, 22, 24, 29, 31, 40, 47, 48, 50, 52, 57};
  static private int[] SENTENCES_endOffsets = {8, 15, 17, 21, 23, 29, 30, 39, 46, 48, 49, 51, 57, 58};

  static private String SENTENCE1 = "Sentence number 1 has 6 words.";
  static private String[] SENTENCE1_punc = {"Sentence", "number", "1", "has", "6", "words", "."};

  @Test
  public void testTokenizer() throws IOException {
    CustomAnalyzer analyzer = CustomAnalyzer.builder(new ClasspathResourceLoader(getClass()))
        .withTokenizer("opennlp", "sentenceModel", "en-test-sent.bin", "tokenizerModel", "en-test-tokenizer.bin")
        .build();
    assertAnalyzesTo(analyzer, SENTENCES, SENTENCES_punc, SENTENCES_startOffsets, SENTENCES_endOffsets);
    assertAnalyzesTo(analyzer, SENTENCE1, SENTENCE1_punc);
  }

  @Test
  public void testTokenizerNoSentenceDetector() throws IOException {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      CustomAnalyzer analyzer = CustomAnalyzer.builder(new ClasspathResourceLoader(getClass()))
          .withTokenizer("opennlp", "tokenizerModel", "en-test-tokenizer.bin")
          .build();
    });
    assertTrue(expected.getMessage().contains("Configuration Error: missing parameter 'sentenceModel'"));
  }

  @Test
  public void testTokenizerNoTokenizer() throws IOException {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      CustomAnalyzer analyzer = CustomAnalyzer.builder(new ClasspathResourceLoader(getClass()))
          .withTokenizer("opennlp", "sentenceModel", "en-test-sent.bin")
          .build();
    });
    assertTrue(expected.getMessage().contains("Configuration Error: missing parameter 'tokenizerModel'"));
  }

  // test analyzer caching the tokenizer
  @Test
  public void testClose() throws IOException {
    Map<String,String> args = new HashMap<String,String>() {{ put("sentenceModel", "en-test-sent.bin");
                                                              put("tokenizerModel", "en-test-tokenizer.bin"); }};
    OpenNLPTokenizerFactory factory = new OpenNLPTokenizerFactory(args);
    factory.inform(new ClasspathResourceLoader(getClass()));

    Tokenizer ts = factory.create(newAttributeFactory());
    ts.setReader(new StringReader(SENTENCES));

    ts.reset();
    ts.close();
    ts.reset();
    ts.setReader(new StringReader(SENTENCES));
    assertTokenStreamContents(ts, SENTENCES_punc);
    ts.close();
    ts.reset();
    ts.setReader(new StringReader(SENTENCES));
    assertTokenStreamContents(ts, SENTENCES_punc);
  }
}
