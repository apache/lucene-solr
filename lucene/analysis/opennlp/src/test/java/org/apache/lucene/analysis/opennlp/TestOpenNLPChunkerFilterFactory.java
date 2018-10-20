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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.payloads.TypeAsPayloadTokenFilterFactory;
import org.apache.lucene.analysis.util.ClasspathResourceLoader;

/**
 * Needs the OpenNLP Tokenizer because it creates full streams of punctuation.
 * Needs the OpenNLP POS tagger for the POS tags.
 *
 * Tagging models are created from tiny test data in opennlp/tools/test-model-data/ and are not very accurate.
 */
public class TestOpenNLPChunkerFilterFactory extends BaseTokenStreamTestCase {

  private static final String SENTENCES = "Sentence number 1 has 6 words. Sentence number 2, 5 words.";
  private static final String[] SENTENCES_punc
      = {"Sentence", "number", "1", "has", "6", "words", ".", "Sentence", "number", "2", ",", "5", "words", "."};
  private static final int[] SENTENCES_startOffsets = {0, 9, 16, 18, 22, 24, 29, 31, 40, 47, 48, 50, 52, 57};
  private static final int[] SENTENCES_endOffsets = {8, 15, 17, 21, 23, 29, 30, 39, 46, 48, 49, 51, 57, 58};
  private static final String[] SENTENCES_chunks
      = { "B-NP", "I-NP", "I-NP", "B-VP", "B-NP", "I-NP", "O", "B-NP", "I-NP", "I-NP", "O", "B-NP", "I-NP", "O" };

  private static final String sentenceModelFile = "en-test-sent.bin";
  private static final String tokenizerModelFile = "en-test-tokenizer.bin";
  private static final String posTaggerModelFile = "en-test-pos-maxent.bin";
  private static final String chunkerModelFile = "en-test-chunker.bin";


  private static byte[][] toPayloads(String... strings) {
    return Arrays.stream(strings).map(s -> s == null ? null : s.getBytes(StandardCharsets.UTF_8)).toArray(byte[][]::new);
  }

  public void testBasic() throws Exception {
    CustomAnalyzer analyzer = CustomAnalyzer.builder(new ClasspathResourceLoader(getClass()))
        .withTokenizer("opennlp", "tokenizerModel", tokenizerModelFile, "sentenceModel", sentenceModelFile)
        .addTokenFilter("opennlpPOS", "posTaggerModel", posTaggerModelFile)
        .addTokenFilter("opennlpChunker", "chunkerModel", chunkerModelFile)
        .build();
    assertAnalyzesTo(analyzer, SENTENCES, SENTENCES_punc, SENTENCES_startOffsets, SENTENCES_endOffsets,
        SENTENCES_chunks, null, null, true);
  }

  public void testPayloads() throws Exception {
    CustomAnalyzer analyzer = CustomAnalyzer.builder(new ClasspathResourceLoader(getClass()))
        .withTokenizer("opennlp", "tokenizerModel", tokenizerModelFile, "sentenceModel", sentenceModelFile)
        .addTokenFilter("opennlpPOS", "posTaggerModel", posTaggerModelFile)
        .addTokenFilter("opennlpChunker", "chunkerModel", chunkerModelFile)
        .addTokenFilter(TypeAsPayloadTokenFilterFactory.class)
        .build();
    assertAnalyzesTo(analyzer, SENTENCES, SENTENCES_punc, SENTENCES_startOffsets, SENTENCES_endOffsets,
        null, null, null, true, toPayloads(SENTENCES_chunks));
  }
}
